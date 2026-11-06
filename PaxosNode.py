"""Single-decree Paxos: all agents are proposers, acceptors and learners."""
import logging
import socket
import threading
from dataclasses import dataclass, field
from typing import Iterable, Optional, TypedDict
import json
import math
import random
import time


class MessageFields(TypedDict, total=False):
    """Additional fields required by particular message kinds; see README."""

    to: int
    accepted_ballot: int
    value: Optional[str]
    acceptors: list[int]
    run_id: str


class Message(MessageFields):
    """Plain JSON dictionary shared by the protocol and UDP transport."""

    type: str
    sender: int
    ballot: int


@dataclass
class ProposerState:
    """State for this agent's current proposal attempt."""

    stage: str = "IDLE"
    ballot: int = -1
    value: Optional[str] = None
    promises: dict[int, Message] = field(default_factory=dict)
    accept_sent: bool = False


@dataclass
class AcceptorState:
    """Promises and accepted values survive changes of proposal round."""

    stage: str = "READY"
    promised: int = -1
    ballot: int = -1
    value: Optional[str] = None


@dataclass
class LearnerState:
    """Votes belong to a ballot/value pair; the first decision is retained."""

    stage: str = "LEARNING"
    votes: dict[tuple[int, str], set[int]] = field(default_factory=dict)
    decided: Optional[str] = None
    ballot: Optional[int] = None
    acceptors: list[int] = field(default_factory=list)


@dataclass
class Paxos:
    """Protocol state only. handle() returns messages to broadcast."""

    node_id: int
    nodes: int
    initial_round: int = 0
    proposer: ProposerState = field(default_factory=ProposerState, init=False)
    acceptor: AcceptorState = field(default_factory=AcceptorState, init=False)
    learner: LearnerState = field(default_factory=LearnerState, init=False)
    majority: int = field(init=False)
    logger: logging.Logger = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        self.logger = logging.getLogger(str(self.node_id))
        self.majority = self.nodes // 2 + 1
        self.log_state("Initialized")

    def log_state(self, event: str) -> None:
        """Snapshot the three roles together so students can follow transitions."""
        proposer = self.proposer
        acceptor = self.acceptor
        learner = self.learner
        self.logger.debug(
            "%s | proposer: ballot=%s value=%r promises=%s/%s accept_sent=%s"
            " | acceptor: promised=%s accepted_ballot=%s accepted_value=%r"
            " | learner: votes=%s decided=%r",
            event, proposer.ballot, proposer.value, sorted(proposer.promises), self.majority,
            proposer.accept_sent, acceptor.promised, acceptor.ballot,
            acceptor.value,
            {key: sorted(voters) for key, voters in learner.votes.items()},
            learner.decided,
        )

    def decision_message(self) -> Message:
        """Evidence for learning: a quorum accepted this ballot and value."""
        # TODO: construct the DECIDE message using the documented wire contract.
        raise NotImplementedError("TODO: decision announcement")

    def propose(self, value: str) -> list[Message]:
        """Start a new attempt with a globally unique integer ballot."""
        if not isinstance(value, str):
            raise ValueError("Values must be strings")
        # TODO: start a proposal attempt and return its PREPARE message.
        return []

    def handle(self, msg: Message) -> list[Message]:
        """Process one message and return outgoing protocol messages."""
        if msg.get("to", self.node_id) != self.node_id:
            return []
        if msg["sender"] not in range(self.nodes):
            return []
        kind = msg["type"]
        if kind == "PREPARE":
            # TODO: implement the acceptor's response to PREPARE.
            pass
        elif kind == "PROMISE":
            # TODO: implement the proposer's response to PROMISE.
            pass
        elif kind == "ACCEPT":
            # TODO: implement the acceptor's response to ACCEPT.
            pass
        elif kind == "ACCEPTED":
            # TODO: implement learning from acceptance votes.
            pass
        elif kind == "DECIDE":
            # TODO: implement learning from a decision announcement.
            pass
        return []


@dataclass
class RetryTimer:
    """A monotonic deadline with capped exponential backoff and small jitter."""

    initial: float = 30.0
    maximum: float = 120.0
    seed: int = 0
    delay: float = field(init=False)
    deadline: Optional[float] = field(default=None, init=False)
    rng: random.Random = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        initial, maximum = self.initial, self.maximum
        if (not math.isfinite(initial) or not math.isfinite(maximum)
                or not 0 < initial <= maximum):
            raise ValueError("retry timeouts must be finite and 0 < timeout <= maximum")
        self.rng = random.Random(self.seed)
        self.delay = initial

    def reset(self, now: float) -> None:
        self.delay = self.initial
        self.deadline = now + self.delay

    def cancel(self) -> None:
        self.deadline = None

    def ready(self, now: float) -> bool:
        return self.deadline is not None and now >= self.deadline

    def backoff(self, now: float) -> float:
        """Schedule the next retry and return its waiting interval."""
        # TODO: implement capped exponential backoff with jitter (see README).
        raise NotImplementedError("TODO: retry backoff")


class PaxosNode(threading.Thread):
    """One UDP agent, with protocol state owned by its receiver thread."""

    def __init__(self, node_id: int, nodes: int, hub_address: tuple[str, int],
                 base_port: int = 5001, value: Optional[str] = None,
                 pitch_after: Optional[threading.Event] = None,
                 retry_timeout: float = 30.0, retry_max: float = 120.0,
                 initial_round: int = 0, run_id: Optional[str] = None) -> None:
        super().__init__(name=str(node_id))
        self.protocol = Paxos(node_id, nodes, initial_round=initial_round)
        self.logger = self.protocol.logger
        self.hub_address = hub_address
        self.run_id = run_id
        self.initial_value: Optional[str] = value
        # Optional ordering of the first pitch, never a wait for consensus.
        self.pitch_after = pitch_after
        self.pitched = threading.Event()
        self.proposal_retry = RetryTimer(retry_timeout, retry_max, seed=node_id * 2)
        self.vote_retry = RetryTimer(retry_timeout, retry_max, seed=node_id * 2 + 1)
        self.last_notification: tuple[str, int, Optional[str]] = ("ACCEPTED", -1, None)
        self.finished = threading.Event()
        self.stopped = threading.Event()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", base_port + node_id))
        self.sock.settimeout(0.1)
        self.logger.debug("Listening on 127.0.0.1:%s; hub=%s:%s",
                         base_port + node_id, *hub_address)

    def broadcast(self, messages: Iterable[Message]) -> None:
        """Send protocol messages through the hub and deliver them locally."""
        # TODO: implement outgoing delivery, including messages produced locally.
        # Keep the caller's message dictionaries unchanged.
        pass

    def retry_messages(self, now: float) -> None:
        """Advance proposal and notification retries using monotonic time."""
        # TODO: manage proposal retries and completion signalling.
        # TODO: manage retransmission of acceptance/decision notifications.
        pass

    def run(self) -> None:
        try:
            reported = False
            while not self.stopped.is_set():
                if self.initial_value is not None and (
                    self.pitch_after is None or self.pitch_after.is_set()
                ):
                    value = self.initial_value
                    self.initial_value = None
                    self.broadcast(self.protocol.propose(value))
                    self.proposal_retry.reset(time.monotonic())
                    self.pitched.set()
                self.retry_messages(time.monotonic())
                if self.protocol.learner.decided is not None and not reported:
                    self.logger.info("DECIDED %r", self.protocol.learner.decided)
                    reported = True
                try:
                    data, address = self.sock.recvfrom(65535)
                except socket.timeout:
                    continue
                if address != self.hub_address:
                    self.logger.debug("Ignoring datagram from non-hub address %s", address)
                    continue
                msg: Message = json.loads(data)
                if self.run_id is not None and msg.get("run_id") != self.run_id:
                    self.logger.debug("Ignoring packet from run=%s", msg.get("run_id"))
                    continue
                self.logger.debug("Received %s", msg)
                self.broadcast(self.protocol.handle(msg))
        except Exception:
            self.logger.exception("Agent failed")
            raise
        finally:
            self.sock.close()
            self.logger.debug("Stopped; socket closed")

    def stop(self) -> None:
        self.logger.debug("Stop requested")
        self.stopped.set()
