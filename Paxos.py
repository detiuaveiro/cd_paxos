"""Launch a fixed group of agents; start Hub.py in another terminal first."""
import argparse
import logging
import math
import random
import uuid
from logging_utils import configure_logging
from PaxosNode import PaxosNode


def main() -> None:
    # This launcher configures the experiment; PaxosNode implements the protocol.
    # The hub and agents must agree on membership size and UDP ports.
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, default=5)
    parser.add_argument("--hub-port", type=int, default=5000)
    parser.add_argument("--base-port", type=int, default=5001)
    parser.add_argument("--scenario", default="race", choices=["race"],
                        help="both dinner proposals compete (default: race)")
    parser.add_argument("--first", choices=["pizza", "hamburger"],
                        help="pitch this meal first, without waiting for a decision")
    parser.add_argument("--seed", type=int,
                        help="seed initial ballot priorities (hub randomness has its own seed)")
    parser.add_argument("--proposer", type=int,
                        help="use a single proposer instead of the dinner competition")
    parser.add_argument("--value", help="single-proposer value (default: Hello Paxos)")
    parser.add_argument("--retry-timeout", type=float, default=30.0,
                        help="initial retry timeout in seconds (default: 30)")
    parser.add_argument("--retry-max", type=float, default=120.0,
                        help="maximum retry interval in seconds (default: 120)")
    parser.add_argument("--keep-running", action="store_true",
                        help="keep accepting messages after everyone has decided")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    args = parser.parse_args()
    # Reject invalid settings before creating threads or opening sockets.
    if args.nodes < 1:
        parser.error("nodes must be positive and proposer must be a valid agent ID")
    if (not math.isfinite(args.retry_timeout) or not math.isfinite(args.retry_max)
            or not 0 < args.retry_timeout <= args.retry_max):
        parser.error("retry timeouts must be finite and 0 < timeout <= maximum")
    first = None
    initial_rounds: dict[int, int] = {}
    # Map proposer IDs to their initial values. Other agents still act as
    # acceptors and learners, but do not initiate a proposal of their own.
    if args.proposer is None and args.value is None:
        if args.nodes < 2:
            parser.error("the dinner competition needs at least two agents")
        proposals = {0: "pizza", 1: "hamburger"}
        # Randomize priority, not the meals: neither preference always gets the higher ballot.
        initial_rounds = dict(zip(proposals, random.Random(args.seed).sample(range(2), 2)))
        first = {"pizza": 0, "hamburger": 1}.get(args.first)
    else:
        # Supplying either option selects the single-proposer experiment.
        proposer = 0 if args.proposer is None else args.proposer
        if not 0 <= proposer < args.nodes:
            parser.error("nodes must be positive and proposer must be a valid agent ID")
        value = "Hello Paxos" if args.value is None else args.value
        proposals = {proposer: value}
    # All agents in this launch share an ID so delayed packets from earlier
    # experiments cannot affect this one, even if the hub stays running.
    run_id = uuid.uuid4().hex
    configure_logging(args.log_level)
    # A ballot combines a round and an agent ID, making it globally unique
    # within this fixed group. Higher ballots have priority, not guaranteed victory.
    ballots = {i: initial_rounds.get(i, 0) * args.nodes + i for i in proposals}
    logging.getLogger("main").info("run=%s n=%s quorum=%s proposals=%s ballots=%s first=%s",
                                    run_id, args.nodes, args.nodes // 2 + 1, proposals, ballots, first)
    agents: list[PaxosNode] = []
    try:
        # Constructing a node binds its socket. Bind all sockets before starting
        # threads so early messages have a listening destination.
        for i in range(args.nodes):
            agents.append(PaxosNode(
                i, args.nodes, ("127.0.0.1", args.hub_port), args.base_port,
                proposals.get(i),
                retry_timeout=args.retry_timeout, retry_max=args.retry_max,
                initial_round=initial_rounds.get(i, 0), run_id=run_id,
            ))
        if first is not None:
            # The second proposer waits only for the first pitch to be sent,
            # not for consensus: both proposals can still compete.
            agents[1 - first].pitch_after = agents[first].pitched
        for agent in agents:
            agent.start()
        if args.keep_running:
            # Leave agents alive to observe retries and decision announcements.
            # join() waits for a thread to exit; Ctrl+C triggers cleanup below.
            for agent in agents:
                agent.join()
        else:
            # A quorum can choose a value before everyone learns it. Keep all
            # agents running until every learner has finished, allowing lagging
            # learners to receive votes or a decision announcement.
            for agent in agents:
                while not agent.finished.wait(0.2):
                    # Detect a failed agent instead of waiting forever for it.
                    if not agent.is_alive():
                        raise RuntimeError(f"{agent.name} stopped before completing consensus")
            logging.getLogger("main").info("CONSENSUS %s (%s/%s)",
                                            agents[0].protocol.learner.decided, args.nodes, args.nodes)
    except KeyboardInterrupt:
        pass
    finally:
        # Request every thread to stop before waiting for any one of them.
        # This also cleans up nodes created before a partial startup failure.
        for agent in agents:
            agent.stop()
        for agent in agents:
            if agent.ident is not None:
                agent.join()
            else:
                # A node whose thread never started cannot close its own socket.
                agent.sock.close()


if __name__ == "__main__":
    main()
