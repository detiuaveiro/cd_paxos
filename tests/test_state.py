"""Role state belongs to one agent and survives only the appropriate changes."""
from dataclasses import is_dataclass

from PaxosNode import Paxos, RetryTimer


def test_dataclass_state_is_not_shared_between_agents():
    first, second = Paxos(0, 3), Paxos(1, 3)
    for state in (first, first.proposer, first.acceptor, first.learner, RetryTimer()):
        assert is_dataclass(state)
    first.proposer.promises[0] = {"accepted_ballot": -1, "value": None}
    first.learner.votes[(0, "pizza")] = {0}
    first.learner.acceptors.append(0)
    assert second.proposer.promises == {}
    assert second.learner.votes == {}
    assert second.learner.acceptors == []


def test_new_attempt_keeps_acceptor_and_learner_history():
    agent = Paxos(0, 3)
    agent.handle(dict(type="ACCEPT", sender=1, ballot=4, value="pizza"))
    agent.handle(dict(type="ACCEPTED", sender=1, ballot=4, value="pizza"))
    acceptor, learner = agent.acceptor, agent.learner
    agent.propose("hamburger")
    assert agent.acceptor is acceptor
    assert (acceptor.promised, acceptor.ballot, acceptor.value) == (4, 4, "pizza")
    assert agent.learner is learner
    assert learner.votes[(4, "pizza")] == {1}
    assert agent.proposer.value == "hamburger"
    assert agent.proposer.promises == {}
