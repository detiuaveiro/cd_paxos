"""Retries use virtual time; no sleeps, sockets, or timing-sensitive assertions."""
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import PaxosNode
from PaxosNode import RetryTimer


@pytest.mark.parametrize("initial,maximum", [
    (0, 1), (-1, 1), (2, 1), (float("nan"), 1),
    (1, float("inf")), (1, float("nan")),
])
def test_retry_timer_rejects_invalid_limits(initial, maximum):
    with pytest.raises(ValueError, match="retry timeouts"):
        RetryTimer(initial, maximum)


def test_backoff_doubles_caps_and_resets():
    timer = RetryTimer(10, 40, seed=7)
    assert not timer.ready(100)
    timer.reset(5)
    assert not timer.ready(14.99)
    assert timer.ready(15)
    for now, ceiling in [(15, 20), (35, 40), (75, 40), (115, 40)]:
        wait = timer.backoff(now)
        assert 0.8 * ceiling <= wait <= ceiling
        assert timer.delay == ceiling
        assert timer.deadline == now + wait
    timer.cancel()
    assert not timer.ready(1000)
    timer.reset(1000)
    assert timer.delay == 10
    assert timer.deadline == 1010


def make_node(monkeypatch, **options):
    sock = Mock()
    monkeypatch.setattr(PaxosNode.socket, "socket", Mock(return_value=sock))
    return PaxosNode.PaxosNode(0, 3, ("127.0.0.1", 6000), 6001,
                               retry_timeout=1, retry_max=4, **options), sock


def test_timeout_restarts_with_higher_ballot_and_preserves_adopted_value(monkeypatch):
    node, sock = make_node(monkeypatch)
    node.broadcast(node.protocol.propose("hamburger"))
    node.protocol.proposer.value = "pizza"  # Value adopted during phase 1.
    node.proposal_retry.reset(0)
    node.retry_messages(0.9)
    assert node.protocol.proposer.ballot == 0
    node.protocol.acceptor.promised = 20  # Another proposer has preempted this attempt.
    node.retry_messages(1)
    assert node.protocol.proposer.ballot > 20
    assert node.protocol.proposer.value == "pizza"
    assert node.proposal_retry.delay == 2
    node.retry_messages(node.proposal_retry.deadline)
    assert node.proposal_retry.delay == 4
    assert json.loads(sock.sendto.call_args_list[-2].args[0])["type"] == "PREPARE"


def test_decision_cancels_proposal_retries_but_keeps_announcing_decision(monkeypatch):
    node, sock = make_node(monkeypatch)
    node.broadcast(node.protocol.propose("pizza"))
    node.proposal_retry.reset(0)
    vote = dict(type="ACCEPTED", sender=1, ballot=0, value="pizza")
    node.broadcast(node.protocol.handle(dict(type="ACCEPT", sender=0, ballot=0, value="pizza")))
    node.protocol.handle(vote)
    node.retry_messages(0)
    assert node.proposal_retry.deadline is None
    sock.sendto.reset_mock()
    node.retry_messages(1)
    sent = [json.loads(call.args[0]) for call in sock.sendto.call_args_list]
    assert sent == [dict(type="DECIDE", sender=0, ballot=0, value="pizza", acceptors=[0, 1])]
    assert node.vote_retry.delay == 2
    # A newer partial acceptance must not replace the chosen decision announcement.
    node.broadcast(node.protocol.handle(dict(type="ACCEPT", sender=1, ballot=5, value="pizza")))
    node.retry_messages(2)
    assert node.vote_retry.delay == 2
    assert node.last_notification == ("DECIDE", 0, "pizza")
    assert node.finished.is_set()


def test_receive_loop_checks_timeouts_without_incoming_messages(monkeypatch):
    node, sock = make_node(monkeypatch, value="pizza")
    clock = SimpleNamespace(now=0)
    monkeypatch.setattr(PaxosNode, "time", SimpleNamespace(monotonic=lambda: clock.now))

    def receive(_size):
        clock.now += 1
        if clock.now >= 4:
            node.stop()
        raise PaxosNode.socket.timeout()

    sock.recvfrom.side_effect = receive
    node.run()
    prepares = [json.loads(call.args[0]) for call in sock.sendto.call_args_list
                if json.loads(call.args[0])["type"] == "PREPARE"]
    assert len(prepares) >= 2
    assert prepares[0]["ballot"] < prepares[-1]["ballot"]
    sock.close.assert_called_once()


def test_race_learners_recover_when_latest_acceptances_are_split(monkeypatch):
    """Both proposers know a choice, but their later partial round split the votes."""
    sockets = [Mock() for _ in range(5)]
    monkeypatch.setattr(PaxosNode.socket, "socket", Mock(side_effect=sockets))
    nodes = [PaxosNode.PaxosNode(i, 5, ("127.0.0.1", 6000), 6001,
                                retry_timeout=1, retry_max=4) for i in range(5)]
    pizza = nodes[0].protocol.propose("pizza")[0]
    hamburger = nodes[1].protocol.propose("hamburger")[0]

    def prepare(request, proposer, acceptors):
        outgoing = []
        for acceptor in acceptors:
            promise = nodes[acceptor].protocol.handle(request)[0]
            outgoing.extend(nodes[proposer].protocol.handle(promise))
        return next(message for message in outgoing if message["type"] == "ACCEPT")

    accept_pizza = prepare(pizza, 0, [0, 2, 3])
    pizza_votes = [nodes[i].protocol.handle(accept_pizza)[0] for i in [0, 2, 3]]
    accept_later = prepare(hamburger, 1, [0, 1, 4])
    assert accept_later["value"] == "pizza"
    for i in [0, 2]:  # The later round reaches only two acceptors, below quorum.
        nodes[i].protocol.handle(accept_later)
    for i in [0, 1]:
        for vote in pizza_votes:
            nodes[i].protocol.handle(vote)
        nodes[i].proposal_retry.reset(0)
    assert [node.protocol.learner.decided for node in nodes] == ["pizza", "pizza", None, None, None]

    # The link is now reliable. Deliver every retry, including generated replies.
    for now in [0, 1, 3, 7, 11]:
        for node in nodes:
            node.retry_messages(now)
        while any(sock.sendto.call_args_list for sock in sockets):
            pending = []
            for sock in sockets:
                pending.extend(json.loads(call.args[0]) for call in sock.sendto.call_args_list)
                sock.sendto.reset_mock()
            for message in pending:
                for node in nodes:
                    node.broadcast(node.protocol.handle(message))
    assert {node.protocol.learner.decided for node in nodes} == {"pizza"}


def test_initial_priority_still_uses_unique_monotonically_increasing_ballots(monkeypatch):
    node, _ = make_node(monkeypatch, initial_round=2)
    first = node.protocol.propose("pizza")[0]["ballot"]
    second = node.protocol.propose("pizza")[0]["ballot"]
    assert first == 6
    assert second > first
    assert first % 3 == second % 3 == node.protocol.node_id
    node.protocol.acceptor.promised = 100
    assert node.protocol.propose("pizza")[0]["ballot"] > 100
