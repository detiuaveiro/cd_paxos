"""Launcher configuration and cleanup, including partial startup failures."""
import runpy
import sys
from unittest.mock import Mock

import pytest

import Paxos
import PaxosNode


@pytest.fixture
def launcher(monkeypatch):
    agents = []
    events = []

    def create(*args, **kwargs):
        agent = Mock()
        agent.ident = None
        agent.arguments = args
        agent.options = kwargs
        index = len(agents)
        events.append(("bind", index))

        def start():
            agent.ident = index + 1
            events.append(("start", index))

        agent.start.side_effect = start
        agents.append(agent)
        return agent

    factory = Mock(side_effect=create)
    monkeypatch.setattr(Paxos, "PaxosNode", factory)
    monkeypatch.setattr(PaxosNode, "PaxosNode", factory)
    monkeypatch.setattr(sys, "argv", ["Paxos.py"])
    return agents, events, factory


def test_launcher_binds_all_agents_before_starting_and_cleans_up(launcher, monkeypatch):
    agents, events, _ = launcher
    monkeypatch.setattr(sys, "argv", ["Paxos.py", "--nodes", "2", "--proposer", "1",
                                     "--hub-port", "6000", "--base-port", "6001",
                                     "--value", "red"])
    Paxos.main()
    assert events == [("bind", 0), ("bind", 1), ("start", 0), ("start", 1)]
    assert [a.arguments for a in agents] == [
        (0, 2, ("127.0.0.1", 6000), 6001, None),
        (1, 2, ("127.0.0.1", 6000), 6001, "red"),
    ]
    for agent in agents:
        agent.stop.assert_called_once()
        assert agent.join.call_count == 1
        agent.sock.close.assert_not_called()


def test_launcher_entry_point_uses_defaults(launcher):
    agents, _, _ = launcher
    runpy.run_module("Paxos", run_name="__main__")
    assert len(agents) == 5
    assert agents[0].arguments == (0, 5, ("127.0.0.1", 5000), 5001, "pizza")
    assert agents[1].arguments == (1, 5, ("127.0.0.1", 5000), 5001, "hamburger")
    assert all(agent.arguments[-1] is None for agent in agents[2:])


@pytest.mark.parametrize("args", [
    ["--nodes", "0"], ["--proposer", "-1"], ["--nodes", "2", "--proposer", "2"],
])
def test_launcher_rejects_invalid_membership(launcher, monkeypatch, args, capsys):
    _, _, factory = launcher
    monkeypatch.setattr(sys, "argv", ["Paxos.py", *args])
    with pytest.raises(SystemExit) as error:
        Paxos.main()
    assert error.value.code == 2
    assert "proposer must be a valid agent ID" in capsys.readouterr().err
    factory.assert_not_called()


def test_keyboard_interrupt_stops_started_agents(launcher):
    agents, _, factory = launcher
    create = factory.side_effect

    def interrupt_join(*args, **kwargs):
        agent = create(*args, **kwargs)
        if args[0] == 0:
            agent.finished.wait.side_effect = KeyboardInterrupt()
        return agent

    factory.side_effect = interrupt_join
    Paxos.main()
    for agent in agents:
        agent.stop.assert_called_once()
        assert agent.join.call_count >= 1


def test_partial_startup_closes_unstarted_sockets(launcher):
    agents, _, factory = launcher
    create = factory.side_effect

    def fail_second_start(*args, **kwargs):
        agent = create(*args, **kwargs)
        if args[0] == 1:
            agent.start.side_effect = RuntimeError("cannot start thread")
        return agent

    factory.side_effect = fail_second_start
    with pytest.raises(RuntimeError, match="cannot start thread"):
        Paxos.main()
    agents[0].join.assert_called_once()
    for agent in agents:
        agent.stop.assert_called_once()
    for agent in agents[1:]:
        agent.sock.close.assert_called_once()
        agent.join.assert_not_called()


@pytest.mark.parametrize("level", ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
def test_launcher_configures_log_level_and_agent_names(launcher, monkeypatch, level):
    configure = Mock()
    monkeypatch.setattr(Paxos.logging, "basicConfig", configure)
    monkeypatch.setattr(sys, "argv", ["Paxos.py", "--log-level", level])
    Paxos.main()
    assert configure.call_args.kwargs["level"] == level
    assert "%(name)s" in configure.call_args.kwargs["format"]
    assert "%(levelname)-8s" in configure.call_args.kwargs["format"]


@pytest.mark.parametrize("args,expected", [
    (["--proposer", "2"], [None, None, "Hello Paxos", None, None]),
    (["--value", "salad"], ["salad", None, None, None, None]),
    (["--nodes", "1", "--value", ""], [""]),
    (["--nodes", "2"], ["pizza", "hamburger"]),
])
def test_competing_and_single_proposer_modes(launcher, monkeypatch, args, expected):
    agents, _, _ = launcher
    monkeypatch.setattr(sys, "argv", ["Paxos.py", *args])
    Paxos.main()
    assert [agent.arguments[-1] for agent in agents] == expected


def test_competition_requires_two_agents(launcher, monkeypatch, capsys):
    _, _, factory = launcher
    monkeypatch.setattr(sys, "argv", ["Paxos.py", "--nodes", "1"])
    with pytest.raises(SystemExit) as error:
        Paxos.main()
    assert error.value.code == 2
    assert "at least two agents" in capsys.readouterr().err
    factory.assert_not_called()


@pytest.mark.parametrize("first,expected", [("pizza", 0), ("hamburger", 1), (None, None)])
def test_race_option_orders_initial_pitch_only(launcher, monkeypatch, first, expected):
    agents, _, _ = launcher
    args = ["Paxos.py", "--scenario", "race"]
    if first is not None:
        args.extend(["--first", first])
    monkeypatch.setattr(sys, "argv", args)
    Paxos.main()
    if expected is not None:
        assert agents[1 - expected].pitch_after is agents[expected].pitched
    else:
        assert all("pitch_after" not in vars(agent) for agent in agents)
    assert [a.arguments[-1] for a in agents] == ["pizza", "hamburger", None, None, None]


@pytest.mark.parametrize("args", [
    ["--retry-timeout", "0"], ["--retry-timeout", "nan"],
    ["--retry-max", "inf"], ["--retry-timeout", "20", "--retry-max", "10"],
])
def test_invalid_retry_options(launcher, monkeypatch, args, capsys):
    _, _, factory = launcher
    monkeypatch.setattr(sys, "argv", ["Paxos.py", *args])
    with pytest.raises(SystemExit) as error:
        Paxos.main()
    assert error.value.code == 2
    assert "retry timeouts" in capsys.readouterr().err
    factory.assert_not_called()


def test_custom_retry_intervals_reach_all_agents(launcher, monkeypatch):
    agents, _, _ = launcher
    monkeypatch.setattr(sys, "argv", ["Paxos.py", "--retry-timeout", "10", "--retry-max", "80"])
    Paxos.main()
    assert all(agent.options["retry_timeout"] == 10 for agent in agents)
    assert all(agent.options["retry_max"] == 80 for agent in agents)



def test_launcher_reports_consensus_and_exits(launcher, caplog):
    agents, _, factory = launcher
    create = factory.side_effect

    def ready(*args, **kwargs):
        agent = create(*args, **kwargs)
        agent.protocol.learner.decided = "hamburger"
        return agent

    factory.side_effect = ready
    with caplog.at_level("INFO", logger="main"):
        Paxos.main()
    assert "CONSENSUS hamburger (5/5)" in caplog.text
    for agent in agents:
        agent.finished.wait.assert_called_once_with(0.2)
        agent.stop.assert_called_once()
        agent.join.assert_called_once()


def test_keep_running_retains_agents_until_interrupted(launcher, monkeypatch):
    agents, _, factory = launcher
    create = factory.side_effect

    def interrupt(*args, **kwargs):
        agent = create(*args, **kwargs)
        if args[0] == 0:
            agent.join.side_effect = [KeyboardInterrupt(), None]
        return agent

    factory.side_effect = interrupt
    monkeypatch.setattr(sys, "argv", ["Paxos.py", "--keep-running"])
    Paxos.main()
    for agent in agents:
        agent.finished.wait.assert_not_called()
        agent.stop.assert_called_once()


def test_keep_running_can_join_without_an_interrupt(launcher, monkeypatch):
    agents, _, _ = launcher
    monkeypatch.setattr(sys, "argv", ["Paxos.py", "--keep-running"])
    Paxos.main()
    for agent in agents:
        assert agent.join.call_count == 2
        agent.finished.wait.assert_not_called()


@pytest.mark.parametrize("alive", [True, False])
def test_waiting_for_consensus_detects_an_agent_that_failed(launcher, alive):
    agents, _, factory = launcher
    create = factory.side_effect

    def waiting(*args, **kwargs):
        agent = create(*args, **kwargs)
        if args[0] == 0:
            agent.finished.wait.side_effect = [False, True]
            agent.is_alive.return_value = alive
        return agent

    factory.side_effect = waiting
    if alive:
        Paxos.main()
    else:
        with pytest.raises(RuntimeError, match="stopped before completing consensus"):
            Paxos.main()
    for agent in agents:
        agent.stop.assert_called_once()


@pytest.mark.parametrize("seed,rounds", [(0, [1, 0]), (1, [0, 1])])
def test_seed_controls_initial_priority_without_swapping_meals(launcher, monkeypatch, seed, rounds):
    agents, _, _ = launcher
    monkeypatch.setattr(sys, "argv", ["Paxos.py", "--seed", str(seed)])
    Paxos.main()
    assert [agent.options["initial_round"] for agent in agents] == rounds + [0, 0, 0]
    assert [agent.arguments[-1] for agent in agents] == ["pizza", "hamburger", None, None, None]


def test_each_launch_uses_one_shared_fresh_run_id(launcher):
    agents, _, _ = launcher
    Paxos.main()
    first_ids = {agent.options["run_id"] for agent in agents}
    assert len(first_ids) == 1
    assert len(first_ids.pop()) == 32
    original = agents[0].options["run_id"]
    count = len(agents)
    Paxos.main()
    second_ids = {agent.options["run_id"] for agent in agents[count:]}
    assert len(second_ids) == 1
    assert original not in second_ids
