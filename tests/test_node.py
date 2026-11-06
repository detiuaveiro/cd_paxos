"""Exercise JSON/UDP handling without opening real sockets."""
import json
import logging
import socket
import threading
from unittest.mock import Mock

import pytest

import PaxosNode


@pytest.fixture
def node_factory(monkeypatch):
    def create(nodes=3, value=None, pitch_after=None, run_id=None):
        sock = Mock()
        monkeypatch.setattr(PaxosNode.socket, "socket", Mock(return_value=sock))
        node = PaxosNode.PaxosNode(0, nodes, ("127.0.0.1", 6000), 6001, value, pitch_after=pitch_after, run_id=run_id)
        return node, sock
    return create


def test_socket_configuration_and_local_delivery(node_factory):
    node, sock = node_factory()
    assert node.name == node.logger.name == "0"
    sock.bind.assert_called_once_with(("127.0.0.1", 6001))
    sock.settimeout.assert_called_once_with(0.1)
    node.broadcast(node.protocol.propose("red"))
    messages = [json.loads(call.args[0]) for call in sock.sendto.call_args_list]
    assert [msg["type"] for msg in messages] == ["PREPARE", "PROMISE"]
    assert node.protocol.acceptor.promised == node.protocol.proposer.ballot
    assert len(node.protocol.proposer.promises) == 1
    assert all(call.args[1] == node.hub_address for call in sock.sendto.call_args_list)


@pytest.mark.parametrize("value", [None, "", "Olá"])
def test_run_receives_only_hub_messages_and_closes(node_factory, caplog, value):
    node, sock = node_factory(nodes=1, value=value)
    accepted = json.dumps(dict(type="ACCEPTED", sender=0, ballot=10,
                               value="received")).encode()
    events = iter([socket.timeout(), (b"not JSON", ("127.0.0.1", 9999)),
                   (accepted, node.hub_address), socket.timeout()])

    def receive(_size):
        try:
            event = next(events)
        except StopIteration:
            node.stop()
            raise socket.timeout()
        if isinstance(event, Exception):
            raise event
        return event

    sock.recvfrom.side_effect = receive
    with caplog.at_level(logging.INFO):
        node.run()
    assert node.protocol.learner.decided == ("received" if value is None else value)
    assert sum(record.message.startswith("DECIDED ") for record in caplog.records) == 1
    assert node.stopped.is_set()
    sock.close.assert_called_once()


def test_socket_is_closed_on_receive_error(node_factory):
    node, sock = node_factory()
    sock.recvfrom.side_effect = OSError("receive failed")
    with pytest.raises(OSError, match="receive failed"):
        node.run()
    sock.close.assert_called_once()


def test_socket_is_closed_on_initial_send_error(node_factory):
    node, sock = node_factory(value="red")
    sock.sendto.side_effect = OSError("send failed")
    with pytest.raises(OSError, match="send failed"):
        node.run()
    sock.close.assert_called_once()


def test_debug_logs_identify_agent_roles_and_state_transitions(node_factory, caplog):
    with caplog.at_level(logging.DEBUG, logger="0"):
        node, _ = node_factory(nodes=1)
        node.broadcast(node.protocol.propose("Olá"))
    records = [record for record in caplog.records if record.name == "0"]
    text = "\n".join(record.message for record in records)
    for event in ["Initialized", "Proposal started", "Prepare promised",
                  "Promise received", "Accept phase started", "Value accepted",
                  "Acceptance vote received"]:
        assert event in text
    assert "proposer: ballot=0 value='Olá' promises=[0]/1 accept_sent=True" in text
    assert "acceptor: promised=0 accepted_ballot=0 accepted_value='Olá'" in text
    assert "learner: votes={(0, 'Olá'): [0]} decided='Olá'" in text
    assert "also delivered locally" in text


def test_info_logs_suppress_state_snapshots(node_factory, caplog):
    with caplog.at_level(logging.INFO, logger="0"):
        node, _ = node_factory(nodes=1)
        node.broadcast(node.protocol.propose("red"))
    assert "PHASE_1_PREPARE" in caplog.text
    assert "PHASE_2_ACCEPT" in caplog.text
    assert "proposer:" not in caplog.text
    assert "PHASE_1_PREPARE" in caplog.text
    assert "PHASE_2_ACCEPT" in caplog.text
    assert "PHASE_1_PROMISED" in caplog.text
    assert "PHASE_2_ACCEPTED" in caplog.text
    assert "stage=DECIDED" in caplog.text
    assert "Sending" not in caplog.text



def test_second_pitch_waits_only_for_first_pitch_not_for_decision(node_factory):
    first_pitched = threading.Event()
    node, sock = node_factory(value="hamburger", pitch_after=first_pitched)
    receives = []

    def receive(_size):
        receives.append(sock.sendto.call_count)
        if len(receives) == 1:
            assert node.protocol.learner.decided is None
            assert not node.pitched.is_set()
            first_pitched.set()
        else:
            assert node.protocol.learner.decided is None
            assert node.pitched.is_set()
            node.stop()
        raise socket.timeout()

    sock.recvfrom.side_effect = receive
    node.run()
    assert receives[0] == 0
    assert receives[1] > 0
    assert node.protocol.proposer.value == "hamburger"
    sock.close.assert_called_once()


def test_stopping_a_waiting_proposer_does_not_pitch(node_factory):
    node, sock = node_factory(value="hamburger", pitch_after=threading.Event())

    def receive(_size):
        node.stop()
        raise socket.timeout()

    sock.recvfrom.side_effect = receive
    node.run()
    sock.sendto.assert_not_called()
    assert not node.pitched.is_set()
    sock.close.assert_called_once()


@pytest.mark.parametrize("kind", ["ACCEPTED", "DECIDE"])
def test_old_or_untagged_packets_cannot_decide_a_new_run(node_factory, kind):
    node, sock = node_factory(nodes=1, run_id="fresh-run")
    stale = dict(type=kind, sender=0, ballot=0, value="hamburger", acceptors=[0])
    current = dict(type="ACCEPTED", sender=0, ballot=0, value="pizza", run_id="fresh-run")
    packets = iter([stale, dict(stale, run_id="old-run"), current])
    states = []

    def receive(_size):
        states.append(node.protocol.learner.decided)
        try:
            return json.dumps(next(packets)).encode(), node.hub_address
        except StopIteration:
            node.stop()
            raise socket.timeout()

    sock.recvfrom.side_effect = receive
    node.run()
    assert states == [None, None, None, "pizza"]
    assert node.protocol.learner.decided == "pizza"
    sent = [json.loads(call.args[0]) for call in sock.sendto.call_args_list]
    assert sent
    assert all(message["run_id"] == "fresh-run" for message in sent)


def test_run_id_is_added_only_to_the_wire_payload(node_factory):
    node, sock = node_factory(run_id="fresh-run")
    msg = node.protocol.propose("pizza")[0]
    node.broadcast([msg])
    assert "run_id" not in msg
    assert json.loads(sock.sendto.call_args_list[0].args[0])["run_id"] == "fresh-run"
