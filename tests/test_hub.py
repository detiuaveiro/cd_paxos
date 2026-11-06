"""Run the real hub loop with a virtual clock and socket."""
from types import SimpleNamespace
from unittest.mock import Mock
import runpy
import logging
import socket
import sys

import pytest

import Hub


class ScheduledSocket:
    """recvfrom advances virtual time instead of waiting for datagrams."""

    def __init__(self):
        self.now = 0.0
        self.timeout = 0.0
        self.incoming = [
            (0.0, b"ignored", ("127.0.0.1", 9999)),
            (0.0, b"first", ("127.0.0.1", 6001)),
            (0.05, b"second", ("127.0.0.1", 6002)),
        ]
        self.sent = []
        self.address = None
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.closed = True

    def bind(self, address):
        self.address = address

    def getsockname(self):
        return self.address

    def settimeout(self, timeout):
        assert timeout > 0
        self.timeout = timeout

    def recvfrom(self, _size):
        if len(self.sent) == 4:
            raise KeyboardInterrupt()
        if self.incoming and self.incoming[0][0] <= self.now + self.timeout:
            self.now, data, sender = self.incoming.pop(0)
            return data, sender
        self.now += self.timeout
        raise socket.timeout()

    def sendto(self, data, destination):
        self.sent.append((self.now, data, destination))


@pytest.mark.parametrize("delay", [0, 0.5])
def test_hub_excludes_sender_and_schedules_independent_deliveries(monkeypatch, delay, caplog):
    caplog.set_level(logging.DEBUG, logger="hub")
    sock = ScheduledSocket()
    monkeypatch.setattr(Hub.socket, "socket", Mock(return_value=sock))
    monkeypatch.setattr(Hub, "time", SimpleNamespace(monotonic=lambda: sock.now))
    delays = [0.4, 0.1, 0.2, 0.0] if delay else [0.0] * 4
    rng = Mock()
    rng.uniform.side_effect = delays
    factory = Mock(return_value=rng)
    monkeypatch.setattr(Hub.random, "Random", factory)
    with pytest.raises(KeyboardInterrupt):
        Hub.run(nodes=3, port=6000, base_port=6001, max_delay=delay, seed=42)
    assert sock.address == ("127.0.0.1", 6000)
    assert sock.closed
    records = [record.message for record in caplog.records if record.name == "hub"]
    assert sum("Packet arrived" in message for message in records) == 3
    assert any("bytes=5 payload=b'first'" in message for message in records)
    assert any("Ignoring packet from unknown sender" in message for message in records)
    scheduled = [message for message in records if "Scheduled delivery" in message]
    forwarded = [message for message in records if "Forwarded delivery" in message]
    assert len(scheduled) == len(forwarded) == 4
    for delivery_id, expected_delay in enumerate(delays, start=1):
        assert f"delivery={delivery_id} " in scheduled[delivery_id - 1]
        assert f"delay={expected_delay:.3f}s" in scheduled[delivery_id - 1]
        assert any(f"delivery={delivery_id} " in message for message in forwarded)
    factory.assert_called_once_with(42)
    assert rng.uniform.call_count == 4
    assert all(call.args == (0, delay) for call in rng.uniform.call_args_list)
    deliveries = {(data, destination): when for when, data, destination in sock.sent}
    assert deliveries == {
        (b"first", ("127.0.0.1", 6002)): pytest.approx(delays[0], abs=0.002),
        (b"first", ("127.0.0.1", 6003)): pytest.approx(delays[1], abs=0.002),
        (b"second", ("127.0.0.1", 6001)): pytest.approx(0.05 + delays[2], abs=0.002),
        (b"second", ("127.0.0.1", 6003)): pytest.approx(0.05 + delays[3], abs=0.002),
    }
    if delay:
        assert [data for _, data, _ in sock.sent] == [b"second", b"first", b"second", b"first"]


@pytest.mark.parametrize("args", [["--nodes", "0"], ["--max-delay", "-1"]])
def test_hub_cli_rejects_invalid_arguments(monkeypatch, args, capsys):
    monkeypatch.setattr(sys, "argv", ["Hub.py", *args])
    with pytest.raises(SystemExit) as error:
        runpy.run_module("Hub", run_name="__main__")
    assert error.value.code == 2
    assert "nodes must be positive" in capsys.readouterr().err


def test_hub_cli_stops_on_keyboard_interrupt(monkeypatch):
    sock = Mock()
    sock.__enter__ = Mock(return_value=sock)
    sock.__exit__ = Mock(return_value=False)
    sock.recvfrom.side_effect = KeyboardInterrupt()
    monkeypatch.setattr(Hub.socket, "socket", Mock(return_value=sock))
    monkeypatch.setattr(sys, "argv", ["Hub.py"])
    runpy.run_module("Hub", run_name="__main__")
    sock.bind.assert_called_once_with(("127.0.0.1", 5000))
    sock.__exit__.assert_called_once()


@pytest.mark.parametrize("level", ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
def test_hub_cli_configures_logging(monkeypatch, level):
    sock = Mock()
    sock.__enter__ = Mock(return_value=sock)
    sock.__exit__ = Mock(return_value=False)
    sock.recvfrom.side_effect = KeyboardInterrupt()
    monkeypatch.setattr(Hub.socket, "socket", Mock(return_value=sock))
    configure = Mock()
    monkeypatch.setattr(Hub.logging, "basicConfig", configure)
    monkeypatch.setattr(sys, "argv", ["Hub.py", "--log-level", level])
    runpy.run_module("Hub", run_name="__main__")
    assert configure.call_args.kwargs["level"] == level
    assert "%(name)s" in configure.call_args.kwargs["format"]


class BoundedSocket(ScheduledSocket):
    """End an experiment at a virtual deadline even if every packet is lost."""

    def __init__(self, end=2.0):
        super().__init__()
        self.end = end

    def recvfrom(self, _size):
        if self.incoming and self.incoming[0][0] <= self.now + self.timeout:
            self.now, data, sender = self.incoming.pop(0)
            return data, sender
        if self.now + self.timeout >= self.end:
            self.now = self.end
            raise KeyboardInterrupt()
        self.now += self.timeout
        raise socket.timeout()


@pytest.fixture
def run_network(monkeypatch):
    def run(issues, sock=None, max_delay=0, seed=42, nodes=3):
        sock = sock or BoundedSocket()
        monkeypatch.setattr(Hub.socket, "socket", Mock(return_value=sock))
        monkeypatch.setattr(Hub, "time", SimpleNamespace(monotonic=lambda: sock.now))
        with pytest.raises(KeyboardInterrupt):
            Hub.run(nodes=nodes, port=6000, base_port=6001, max_delay=max_delay,
                    seed=seed, issues=issues)
        assert sock.closed
        return sock
    return run


@pytest.mark.parametrize("rate,expected", [(0.0, 4), (1.0, 0), (0.5, 2)])
def test_random_loss_per_destination(run_network, caplog, rate, expected):
    caplog.set_level(logging.INFO, logger="hub")
    sock = run_network(Hub.NetworkIssues(drop_rate=rate))
    assert len(sock.sent) == expected
    assert caplog.text.count("reason=random loss") == 4 - expected


@pytest.mark.parametrize("rate,expected", [(0.0, 4), (1.0, 8), (0.5, 6)])
def test_duplicate_copies_keep_original_bytes(run_network, caplog, rate, expected):
    caplog.set_level(logging.INFO, logger="hub")
    sock = run_network(Hub.NetworkIssues(duplicate_rate=rate))
    assert len(sock.sent) == expected
    assert caplog.text.count("Duplicated from=") == expected - 4
    assert {data for _, data, _ in sock.sent} == {b"first", b"second"}


def test_partition_blocks_both_directions_then_heals(run_network, caplog):
    caplog.set_level(logging.INFO, logger="hub")
    sock = BoundedSocket()
    sock.incoming = [(0, b"isolated", ("127.0.0.1", 6001)),
                     (0.1, b"majority", ("127.0.0.1", 6002)),
                     (1, b"healed", ("127.0.0.1", 6001))]
    run_network(Hub.NetworkIssues(partition_nodes=(0,), partition_duration=1), sock)
    assert [(data, address[1]) for _, data, address in sock.sent] == [
        (b"majority", 6003), (b"healed", 6002), (b"healed", 6003)]
    assert caplog.text.count("reason=partition") == 3


def test_outage_repeats_and_drops_queued_deliveries(run_network, caplog, monkeypatch):
    rng = Mock()
    rng.uniform.return_value = 0.55
    monkeypatch.setattr(Hub.random, "Random", Mock(return_value=rng))
    caplog.set_level(logging.INFO, logger="hub")
    sock = BoundedSocket(end=3)
    sock.incoming = [(0, b"outage", ("127.0.0.1", 6001)),
                     (0.5, b"queued", ("127.0.0.1", 6001)),
                     (1.1, b"outage-again", ("127.0.0.1", 6001))]
    # Both queued copies are due during the next outage (1.05).
    run_network(Hub.NetworkIssues(outage_period=1, outage_duration=0.2), sock, 1)
    assert sock.sent == []
    assert caplog.text.count("reason=outage") == 6
    assert "Dropped delivery=" in caplog.text


@pytest.mark.parametrize("kwargs", [
    {"drop_rate": -0.1}, {"drop_rate": 1.1}, {"duplicate_rate": float("nan")},
    {"duplicate_rate": float("inf")}, {"partition_duration": -1},
    {"outage_period": float("inf")}, {"outage_duration": float("nan")},
    {"outage_duration": 1}, {"outage_period": 1, "outage_duration": 2},
])
def test_fault_settings_reject_invalid_values(kwargs):
    with pytest.raises(ValueError):
        Hub.NetworkIssues(**kwargs)


@pytest.mark.parametrize("args,message", [
    (["--drop-rate", "2"], "between 0 and 1"),
    (["--duplicate-rate", "-1"], "between 0 and 1"),
    (["--partition-nodes", "5"], "valid agent IDs"),
    (["--partition-nodes", "-1"], "valid agent IDs"),
    (["--outage-period", "2", "--outage-duration", "3"], "must not exceed"),
    (["--max-delay", "nan"], "max-delay nonnegative"),
])
def test_cli_rejects_invalid_network_settings(monkeypatch, args, message, capsys):
    monkeypatch.setattr(sys, "argv", ["Hub.py", *args])
    with pytest.raises(SystemExit) as error:
        runpy.run_module("Hub", run_name="__main__")
    assert error.value.code == 2
    assert message in capsys.readouterr().err


def test_cli_passes_fault_options_to_network(monkeypatch):
    sock = Mock()
    sock.__enter__ = Mock(return_value=sock)
    sock.__exit__ = Mock(return_value=False)
    sock.recvfrom.side_effect = KeyboardInterrupt()
    monkeypatch.setattr(Hub.socket, "socket", Mock(return_value=sock))
    cap = Mock()
    monkeypatch.setattr(Hub.logging, "basicConfig", cap)
    info = Mock()
    monkeypatch.setattr(Hub.logger, "info", info)
    monkeypatch.setattr(sys, "argv", ["Hub.py", "--drop-rate", "0.2",
        "--duplicate-rate", "0.3", "--partition-nodes", "0", "1",
        "--partition-duration", "8", "--outage-period", "10", "--outage-duration", "2"])
    runpy.run_module("Hub", run_name="__main__")
    issues = info.call_args_list[1].args[1]
    assert vars(issues) == vars(Hub.NetworkIssues(0.2, 0.3, (0, 1), 8, 10, 2))


class ConsensusSocket(BoundedSocket):
    """Connect the actual hub to three Paxos state machines in virtual time."""

    def __init__(self, retry_at=None, proposals=((0, "red"),)):
        import json
        from PaxosNode import Paxos

        super().__init__(end=6)
        self.json = json
        self.agents = [Paxos(i, 3) for i in range(3)]
        self.incoming = []
        prepared = [(sender, self.agents[sender].propose(value))
                    for sender, value in proposals]
        for sender, messages in prepared:
            self.broadcast(sender, messages)
        if retry_at is not None:
            self.incoming.append((retry_at, self.retry, None))
        self.incoming.sort(key=lambda event: event[0])

    def retry(self):
        self.broadcast(1, self.agents[1].propose("blue"))

    def broadcast(self, sender, messages):
        pending = list(messages)
        while pending:
            msg = pending.pop(0)
            self.incoming.append((self.now, self.json.dumps(msg).encode(),
                                  ("127.0.0.1", 6001 + sender)))
            pending.extend(self.agents[sender].handle(msg))
        self.incoming.sort(key=lambda event: event[0])

    def recvfrom(self, size):
        if self.incoming and self.incoming[0][0] <= self.now + self.timeout:
            if callable(self.incoming[0][1]):
                self.now, callback, _ = self.incoming.pop(0)
                callback()
                return self.recvfrom(size)
        return super().recvfrom(size)

    def sendto(self, data, destination):
        super().sendto(data, destination)
        receiver = destination[1] - 6001
        replies = self.agents[receiver].handle(self.json.loads(data))
        self.broadcast(receiver, replies)


@pytest.mark.parametrize("issues,expected", [
    (Hub.NetworkIssues(duplicate_rate=1), ["red", "red", "red"]),
    (Hub.NetworkIssues(drop_rate=1), [None, None, None]),
    (Hub.NetworkIssues(partition_nodes=(2,), partition_duration=10), ["red", "red", None]),
    (Hub.NetworkIssues(partition_nodes=(0,), partition_duration=10), [None, None, None]),
])
def test_paxos_over_faulty_hub_agrees_or_waits_for_majority(run_network, issues, expected):
    sock = ConsensusSocket()
    run_network(issues, sock, max_delay=0.2)
    assert [agent.learner.decided for agent in sock.agents] == expected


@pytest.mark.parametrize("issues,expected", [
    (Hub.NetworkIssues(partition_nodes=(2,), partition_duration=2), "red"),
    (Hub.NetworkIssues(outage_period=10, outage_duration=2), "blue"),
])
def test_manual_retry_after_healing_reaches_consensus(run_network, issues, expected):
    sock = ConsensusSocket(retry_at=2.1)
    run_network(issues, sock, max_delay=0.2)
    assert {agent.learner.decided for agent in sock.agents} == {expected}
    assert sock.agents[1].proposer.value == expected


@pytest.mark.parametrize("duplicate_rate", [0, 0.5, 1])
def test_dinner_proposals_compete_through_delayed_duplicating_hub(run_network, duplicate_rate):
    sock = ConsensusSocket(proposals=((0, "pizza"), (1, "hamburger")))
    run_network(Hub.NetworkIssues(duplicate_rate=duplicate_rate), sock, max_delay=0.2)
    dinner = sock.agents[0].learner.decided
    assert dinner in {"pizza", "hamburger"}
    assert all(agent.learner.decided == dinner for agent in sock.agents)


class RetryingConsensusSocket(ConsensusSocket):
    """Exercise real node retry/broadcast code through the actual hub loop."""

    def __init__(self, lost_type=None, lose_learner=False, competing=False, nodes=3):
        import json
        from unittest.mock import patch
        from PaxosNode import PaxosNode

        BoundedSocket.__init__(self, end=12)
        self.json = json
        self.incoming = []
        self.nodes = []
        self.lost_type = lost_type
        self.lose_learner = lose_learner
        self.lost = 0
        for sender in range(nodes):
            sock = Mock()
            sock.sendto.side_effect = lambda data, _address, sender=sender: self.submit(sender, data)
            with patch("PaxosNode.socket.socket", return_value=sock):
                self.nodes.append(PaxosNode(sender, nodes, ("127.0.0.1", 6000), 6001,
                                            retry_timeout=0.3, retry_max=2))
        self.agents = [node.protocol for node in self.nodes]
        self.broadcast(0, self.agents[0].propose("pizza"))
        self.nodes[0].proposal_retry.reset(0)
        if competing:
            self.broadcast(1, self.agents[1].propose("hamburger"))
            self.nodes[1].proposal_retry.reset(0)
        for step in range(1, 120):
            self.incoming.append((step / 10, self.tick, None))
        self.incoming.sort(key=lambda event: event[0])

    def submit(self, sender, data):
        self.incoming.append((self.now, data, ("127.0.0.1", 6001 + sender)))
        self.incoming.sort(key=lambda event: event[0])

    def broadcast(self, sender, messages):
        self.nodes[sender].broadcast(messages)

    def tick(self):
        for node in self.nodes:
            node.retry_messages(self.now)

    def sendto(self, data, destination):
        message = self.json.loads(data)
        # Omit all remote messages of one kind in the initial attempt.
        initial_phase_lost = message["type"] == self.lost_type and message["ballot"] == 0
        learner_lost = (self.lose_learner and message["type"] == "ACCEPTED"
                        and destination[1] == 6003 and self.now < 0.5)
        if initial_phase_lost or learner_lost:
            self.lost += 1
            return
        super().sendto(data, destination)


@pytest.mark.parametrize("kind", ["PREPARE", "PROMISE", "ACCEPT", "ACCEPTED"])
def test_automatic_retry_recovers_a_lost_protocol_phase(run_network, kind):
    sock = RetryingConsensusSocket(lost_type=kind)
    run_network(Hub.NetworkIssues(), sock, max_delay=0.05)
    assert sock.lost > 0
    assert {agent.learner.decided for agent in sock.agents} == {"pizza"}
    assert sock.agents[0].proposer.ballot > 0
    assert sock.nodes[0].proposal_retry.deadline is None


def test_acceptor_retries_recover_a_learner_that_missed_the_decision(run_network):
    sock = RetryingConsensusSocket(lose_learner=True)
    run_network(Hub.NetworkIssues(), sock, max_delay=0.05)
    assert sock.lost > 0
    assert {agent.learner.decided for agent in sock.agents} == {"pizza"}
    assert sock.agents[0].proposer.ballot == 0  # Recovery did not need another proposal.


def test_automatic_retry_survives_a_temporary_network_outage(run_network):
    sock = RetryingConsensusSocket()
    run_network(Hub.NetworkIssues(outage_period=20, outage_duration=1), sock, max_delay=0.05)
    assert {agent.learner.decided for agent in sock.agents} == {"pizza"}
    assert sock.agents[0].proposer.ballot > 0


def test_retries_do_not_invent_a_quorum_during_a_permanent_partition(run_network):
    sock = RetryingConsensusSocket()
    run_network(Hub.NetworkIssues(partition_nodes=(0,), partition_duration=100),
                sock, max_delay=0.05)
    assert {agent.learner.decided for agent in sock.agents} == {None}
    assert sock.agents[0].proposer.ballot > 0
    assert sock.nodes[0].proposal_retry.delay == 2



def test_competing_retrying_proposers_agree_after_an_outage(run_network):
    sock = RetryingConsensusSocket(competing=True)
    run_network(Hub.NetworkIssues(outage_period=20, outage_duration=1), sock, max_delay=0.05)
    dinner = sock.agents[0].learner.decided
    assert dinner in {"pizza", "hamburger"}
    assert all(agent.learner.decided == dinner for agent in sock.agents)
    assert all(node.proposal_retry.deadline is None for node in sock.nodes[:2])


@pytest.mark.parametrize("seed", range(20))
def test_five_agent_race_finishes_after_loss_duplication_and_outage(run_network, seed):
    sock = RetryingConsensusSocket(competing=True, nodes=5)
    run_network(Hub.NetworkIssues(drop_rate=0.1, duplicate_rate=0.3,
                                  outage_period=20, outage_duration=1),
                sock, max_delay=0.2, seed=seed, nodes=5)
    dinner = sock.agents[0].learner.decided
    assert dinner in {"pizza", "hamburger"}
    assert all(agent.learner.decided == dinner for agent in sock.agents)
    assert all(node.finished.is_set() for node in sock.nodes)
