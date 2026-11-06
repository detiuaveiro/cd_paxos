"""Paxos safety and progress with deterministic, reordered deliveries."""
import random

import pytest

from PaxosNode import Paxos


def deliver(agents, messages, seed=0):
    rng = random.Random(seed)
    pending = [(i, msg) for msg in messages for i in range(len(agents))]
    while pending:
        i, msg = pending.pop(rng.randrange(len(pending)))
        for reply in agents[i].handle(msg):
            pending.extend((j, reply) for j in range(len(agents)))


@pytest.mark.parametrize("seed", range(50))
def test_reordered_competing_proposals_agree(seed):
    agents = [Paxos(i, 3) for i in range(3)]
    deliver(agents, agents[0].propose("red") + agents[1].propose("blue"), seed)
    decisions = {agent.learner.decided for agent in agents}
    assert len(decisions) == 1
    assert decisions <= {"red", "blue"}


@pytest.mark.parametrize("size", [1, 2, 3, 4, 5])
@pytest.mark.parametrize("value", ["red", "", "Olá, Paxos!"])
def test_every_agent_learns_and_later_proposer_preserves_value(size, value):
    agents = [Paxos(i, size) for i in range(size)]
    deliver(agents, agents[0].propose(value))
    assert {agent.learner.decided for agent in agents} == {value}
    deliver(agents, agents[-1].propose("replacement"))
    assert agents[-1].proposer.value == value
    assert {agent.acceptor.value for agent in agents} == {value}


@pytest.mark.parametrize("value", [None, 1, [], {}])
def test_values_must_be_strings(value):
    with pytest.raises(ValueError, match="strings"):
        Paxos(0, 3).propose(value)


def test_ballots_are_unique_and_retries_exceed_observed_promises():
    agents = [Paxos(i, 3) for i in range(3)]
    ballots = [a.propose("red")[0]["ballot"] for a in agents]
    assert len(set(ballots)) == 3
    agent = agents[0]
    agent.handle(dict(type="PREPARE", sender=1, ballot=100))
    agent.proposer.promises[1] = {}
    agent.proposer.accept_sent = True
    retry = agent.propose("blue")[0]
    assert retry["ballot"] > 100
    assert retry["ballot"] % 3 == agent.node_id
    assert agent.proposer.promises == {}
    assert not agent.proposer.accept_sent
    assert agent.propose("green")[0]["ballot"] > retry["ballot"]


@pytest.mark.parametrize("fields", [{"to": 2}, {"sender": -1}, {"sender": 3}])
def test_messages_for_other_agents_or_from_unknown_members_are_ignored(fields):
    agent = Paxos(0, 3)
    msg = dict(type="PREPARE", sender=1, ballot=10)
    msg.update(fields)
    assert agent.handle(msg) == []
    assert agent.acceptor.promised == -1


def test_acceptor_promises_report_accepted_state_and_reject_stale_ballots():
    agent = Paxos(0, 3)
    accepted = agent.handle(dict(type="ACCEPT", sender=1, ballot=5, value="red"))
    assert accepted == [dict(type="ACCEPTED", sender=0, ballot=5, value="red")]
    prepare = dict(type="PREPARE", sender=2, ballot=10)
    reply = agent.handle(prepare)
    assert reply == [dict(type="PROMISE", sender=0, to=2, ballot=10,
                          accepted_ballot=5, value="red")]
    assert agent.handle(prepare) == reply  # Repeated prepare is idempotent.
    assert agent.handle(dict(type="PREPARE", sender=1, ballot=9)) == []
    assert agent.handle(dict(type="ACCEPT", sender=1, ballot=9, value="blue")) == []
    assert (agent.acceptor.promised, agent.acceptor.ballot, agent.acceptor.value) == (10, 5, "red")


def test_duplicate_and_stale_promises_do_not_start_another_accept_phase():
    agent = Paxos(0, 3)
    agent.propose("red")
    msg = dict(type="PROMISE", sender=1, ballot=agent.proposer.ballot,
               accepted_ballot=-1, value=None)
    assert agent.handle(dict(msg, ballot=agent.proposer.ballot - 1)) == []
    for _ in range(5):
        assert agent.handle(msg) == []
    assert not agent.proposer.accept_sent
    assert agent.handle(dict(msg, sender=2)) == [
        dict(type="ACCEPT", sender=0, ballot=agent.proposer.ballot, value="red")]
    assert agent.handle(msg) == []


def test_proposer_selects_highest_accepted_ballot():
    agent = Paxos(0, 5)
    agent.acceptor.promised = 20
    agent.propose("new")
    for sender, accepted_ballot, value in [(1, 8, "latest"), (2, 3, "older")]:
        assert agent.handle(dict(type="PROMISE", sender=sender, ballot=agent.proposer.ballot,
                                 accepted_ballot=accepted_ballot, value=value)) == []
    outgoing = agent.handle(dict(type="PROMISE", sender=3, ballot=agent.proposer.ballot,
                                 accepted_ballot=-1, value=None))
    assert outgoing[0]["value"] == "latest"


def test_votes_require_distinct_acceptors_for_the_same_ballot_and_value():
    agent = Paxos(0, 3)
    msg = dict(type="ACCEPTED", sender=1, ballot=1, value="red")
    for _ in range(5):
        agent.handle(msg)
    agent.handle(dict(msg, sender=2, ballot=2))
    agent.handle(dict(msg, sender=2, value="blue"))
    assert agent.learner.decided is None
    agent.handle(dict(msg, sender=2))
    assert agent.learner.decided == "red"
    agent.handle(dict(msg, sender=0, ballot=2, value="blue"))
    agent.handle(dict(msg, sender=1, ballot=2, value="blue"))
    assert agent.learner.decided == "red"  # A learner reports its decision only once.


def test_no_decision_without_majority():
    agent = Paxos(0, 3)
    pending = agent.propose("red")
    while pending:
        pending.extend(agent.handle(pending.pop(0)))
    assert agent.learner.decided is None
    assert not agent.proposer.accept_sent


def test_majority_can_choose_without_all_agents():
    agents = [Paxos(i, 3) for i in range(3)]
    pending = agents[0].propose("red")
    while pending:
        msg = pending.pop(0)
        for agent in agents[:2]:  # Agent 2 is unavailable.
            pending.extend(agent.handle(msg))
    assert [agent.learner.decided for agent in agents] == ["red", "red", None]


def test_roles_show_independent_stages_and_quorum_progress(caplog):
    import logging

    with caplog.at_level(logging.INFO, logger="0"):
        agent = Paxos(0, 3)
        assert (agent.proposer.stage, agent.acceptor.stage, agent.learner.stage) == (
            "IDLE", "READY", "LEARNING")
        prepare = agent.propose("red")[0]
        promise = agent.handle(prepare)[0]
        assert agent.handle(promise) == []
        assert "promises=1/2" in caplog.text
        accept = agent.handle(dict(promise, sender=1))[0]
        assert agent.proposer.stage == "PHASE_2_ACCEPT"
        vote = agent.handle(accept)[0]
        agent.handle(vote)
        assert agent.learner.stage == "LEARNING"
        assert "votes=1/2 stage=LEARNING" in caplog.text
        agent.handle(dict(vote, sender=1))
        assert agent.learner.stage == "DECIDED"
        assert agent.learner.decided == "red"
        # Learning a decision does not disable the acceptor role.
        agent.handle(dict(type="PREPARE", sender=2, ballot=10))
        assert agent.acceptor.stage == "PHASE_1_PROMISED"
        assert agent.learner.stage == "DECIDED"
        assert agent.proposer.stage == "PHASE_2_ACCEPT"


@pytest.mark.parametrize("acceptors", [[0, 0], [0], [0, 3], [-1, 0]])
def test_decision_announcement_requires_a_quorum_of_valid_distinct_acceptors(acceptors):
    agent = Paxos(2, 3)
    assert agent.handle(dict(type="DECIDE", sender=0, ballot=0,
                             value="pizza", acceptors=acceptors)) == []
    assert agent.learner.decided is None


def test_decision_announcement_is_relayed_once_and_never_changes_a_decision():
    agent = Paxos(2, 3)
    announcement = dict(type="DECIDE", sender=0, ballot=0, value="pizza", acceptors=[0, 1])
    assert agent.handle(announcement) == [dict(announcement, sender=2)]
    assert agent.learner.ballot == 0
    assert agent.learner.acceptors == [0, 1]
    assert agent.handle(announcement) == []
    assert agent.learner.decided == "pizza"
    # A higher-ballot certificate also supplies votes for an already decided challenger.
    agent.handle(dict(announcement, ballot=5))
    assert agent.learner.votes[(5, "pizza")] == {0, 1}
    assert agent.learner.ballot == 0
