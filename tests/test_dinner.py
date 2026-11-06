"""Two competing dinner suggestions: Paxos must choose one shared meal."""
import pytest

from PaxosNode import Paxos
from test_paxos import deliver


@pytest.mark.parametrize("nodes", [2, 3, 5],
                         ids=["two-agents", "three-agents", "five-agents"])
@pytest.mark.parametrize("seed", range(20))
def test_pizza_and_hamburger_compete_with_reordered_messages(nodes, seed):
    agents = [Paxos(i, nodes) for i in range(nodes)]
    # Both proposals exist before any network message is delivered.
    pizza = agents[0].propose("pizza")
    hamburger = agents[1].propose("hamburger")
    deliver(agents, pizza + hamburger, seed)

    dinner = agents[0].learner.decided
    assert dinner in {"pizza", "hamburger"}
    assert all(agent.learner.decided == dinner for agent in agents)
    assert all(agent.learner.stage == "DECIDED" for agent in agents)


@pytest.mark.parametrize("first", [0, 1], ids=["pizza-first", "hamburger-first"])
def test_first_chosen_dinner_survives_competing_proposal_and_retry(first):
    agents = [Paxos(i, 2) for i in range(2)]
    preferences = ["pizza", "hamburger"]
    proposals = [agent.propose(meal) for agent, meal in zip(agents, preferences)]
    other = 1 - first

    # Network delays let one proposal finish before the other's PREPARE arrives.
    deliver(agents, proposals[first])
    assert {agent.learner.decided for agent in agents} == {preferences[first]}
    deliver(agents, proposals[other])
    # Even a higher-ballot retry cannot replace an already chosen dinner.
    deliver(agents, agents[other].propose(preferences[other]))
    assert agents[other].proposer.value == preferences[first]
    assert {agent.learner.decided for agent in agents} == {preferences[first]}
    assert {agent.acceptor.value for agent in agents} == {preferences[first]}


def test_two_agents_cannot_choose_different_dinners_while_disconnected():
    agents = [Paxos(i, 2) for i in range(2)]
    meals = ["pizza", "hamburger"]
    for agent, meal in zip(agents, meals):
        pending = agent.propose(meal)
        while pending:
            pending.extend(agent.handle(pending.pop(0)))
        assert len(agent.proposer.promises) == 1
        assert agent.majority == 2
        assert agent.learner.decided is None

    # After reconnection, a new proposal lets both agents agree.
    deliver(agents, agents[1].propose("hamburger"))
    assert {agent.learner.decided for agent in agents} == {"hamburger"}


@pytest.mark.parametrize("seed", range(20))
@pytest.mark.parametrize("first_meal,challenge", [
    ("pizza", "hamburger"), ("hamburger", "pizza"),
])
def test_controlled_five_agent_dinner_has_predictable_outcome(seed, first_meal, challenge):
    agents = [Paxos(i, 5) for i in range(5)]
    first = 0 if first_meal == "pizza" else 1
    challenger = 1 - first
    deliver(agents, agents[first].propose(first_meal), seed)
    assert agents[challenger].learner.decided == first_meal
    deliver(agents, agents[challenger].propose(challenge), seed + 1)
    assert agents[challenger].proposer.value == first_meal
    assert {agent.learner.decided for agent in agents} == {first_meal}
    assert {agent.acceptor.value for agent in agents} == {first_meal}


@pytest.mark.parametrize("first", [0, 1], ids=["pizza-pitched-first", "hamburger-pitched-first"])
@pytest.mark.parametrize("winner", [0, 1], ids=["pizza-wins", "hamburger-wins"])
def test_first_pitch_does_not_force_the_winning_meal(first, winner):
    agents = [Paxos(i, 5) for i in range(5)]
    meals = ["pizza", "hamburger"]
    # Both pitches occur before a decision; the network can favour either one.
    proposals = {}
    for proposer in [first, 1 - first]:
        proposals[proposer] = agents[proposer].propose(meals[proposer])
    assert all(agent.learner.decided is None for agent in agents)
    deliver(agents, proposals[winner])
    deliver(agents, proposals[1 - winner])
    assert {agent.learner.decided for agent in agents} == {meals[winner]}


def run_delayed_dinner(network_seed, initial_rounds):
    """Realistic independent delays plus immediate local delivery, without sleeps."""
    import heapq
    import random
    from collections import deque

    agents = [Paxos(i, 5, initial_round=initial_rounds.get(i, 0)) for i in range(5)]
    rng, events = random.Random(network_seed), []
    now, sequence = 0.0, 0

    def broadcast(sender, messages):
        nonlocal sequence
        pending = deque(messages)
        while pending:
            msg = pending.popleft()
            for destination in range(5):
                if destination != sender:
                    sequence += 1
                    heapq.heappush(events, (now + rng.uniform(0, 5), sequence, destination, msg))
            pending.extend(agents[sender].handle(msg))

    for sender, meal in [(0, "pizza"), (1, "hamburger")]:
        broadcast(sender, agents[sender].propose(meal))
    while events:
        now, _, destination, msg = heapq.heappop(events)
        broadcast(destination, agents[destination].handle(msg))
    decisions = {agent.learner.decided for agent in agents}
    assert len(decisions) == 1
    assert decisions <= {"pizza", "hamburger"}
    return agents[0].learner.decided


@pytest.mark.parametrize("network_seed", range(20))
@pytest.mark.parametrize("priority,winner", [({0: 1, 1: 0}, "pizza"), ({0: 0, 1: 1}, "hamburger")])
def test_either_meal_can_win_with_independent_network_delays(network_seed, priority, winner):
    # These deterministic schedules reproduce the old practical bias toward ballot 1.
    # Reversing initial ballot priority must also allow pizza to win.
    assert run_delayed_dinner(network_seed, priority) == winner
