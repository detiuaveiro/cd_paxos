"""UDP classroom network: delay, loss, duplication, partitions and outages."""
import argparse
import heapq
import logging
from logging_utils import configure_logging
import random
import socket
import time
import math
from dataclasses import dataclass
from typing import Optional


logger = logging.getLogger("hub")

@dataclass
class NetworkIssues:
    """Fault settings. Rates are per destination, not per incoming broadcast."""

    drop_rate: float = 0.0
    duplicate_rate: float = 0.0
    partition_nodes: tuple[int, ...] = ()
    partition_duration: float = 10.0
    outage_period: float = 0.0
    outage_duration: float = 0.0

    def __post_init__(self) -> None:
        for rate in (self.drop_rate, self.duplicate_rate):
            if not 0 <= rate <= 1:
                raise ValueError("drop-rate and duplicate-rate must be between 0 and 1")
        for duration in (self.partition_duration, self.outage_period, self.outage_duration):
            if not math.isfinite(duration) or duration < 0:
                raise ValueError("fault durations must be finite and nonnegative")
        if self.outage_duration > self.outage_period:
            raise ValueError("outage-duration must not exceed outage-period")

    def blocked(self, sender_id: int, destination_id: int, elapsed: float) -> str:
        """Temporary partitions split members into two groups; outages block all links."""
        if self.outage_period > 0 and elapsed % self.outage_period < self.outage_duration:
            return "outage"
        if elapsed < self.partition_duration and (
            (sender_id in self.partition_nodes) != (destination_id in self.partition_nodes)
        ):
            return "partition"
        return ""


def run(nodes: int = 5, port: int = 5000, base_port: int = 5001,
        max_delay: float = 5.0, seed: Optional[int] = None,
        issues: Optional[NetworkIssues] = None) -> None:
    if issues is None:
        issues = NetworkIssues()
    rng = random.Random(seed)
    agents = [("127.0.0.1", base_port + i) for i in range(nodes)]
    pending: list[tuple[float, int, bytes, tuple[str, int], tuple[str, int]]] = []
    sequence = 0
    started = time.monotonic()
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", port))
        logger.info("Hub listening on %s; delay range=0..%.3fs",
                    sock.getsockname(), max_delay)
        logger.info("Network issues: %s; timers start when the hub starts", issues)
        while True:
            now = time.monotonic()
            while pending and pending[0][0] <= now:
                deadline, delivery_id, data, sender, destination = heapq.heappop(pending)
                reason = issues.blocked(agents.index(sender), agents.index(destination),
                                        time.monotonic() - started)
                if reason:
                    logger.info("Dropped delivery=%s from=%s to=%s reason=%s",
                                delivery_id, sender, destination, reason)
                    continue
                sock.sendto(data, destination)
                logger.debug("Forwarded delivery=%s to=%s bytes=%s lateness=%.3fs",
                             delivery_id, destination, len(data),
                             time.monotonic() - deadline)
            # Scheduling deliveries avoids blocking reception during a delay.
            timeout = max(0, pending[0][0] - time.monotonic()) if pending else 0.1
            sock.settimeout(max(timeout, 0.001))
            try:
                data, sender = sock.recvfrom(65535)
            except socket.timeout:
                continue
            logger.debug("Packet arrived from=%s bytes=%s payload=%r", sender, len(data), data)
            if sender not in agents:
                logger.debug("Ignoring packet from unknown sender=%s", sender)
                continue
            for destination in agents:
                if destination == sender:
                    continue
                reason = issues.blocked(agents.index(sender), agents.index(destination),
                                        time.monotonic() - started)
                if reason:
                    logger.info("Dropped from=%s to=%s reason=%s", sender, destination, reason)
                    continue
                if issues.drop_rate > 0 and rng.random() < issues.drop_rate:
                    logger.info("Dropped from=%s to=%s reason=random loss", sender, destination)
                    continue
                copies = 1
                if issues.duplicate_rate > 0 and rng.random() < issues.duplicate_rate:
                    copies = 2
                    logger.info("Duplicated from=%s to=%s", sender, destination)
                for copy in range(copies):
                    delay = rng.uniform(0, max_delay)
                    sequence += 1
                    logger.debug("Scheduled delivery=%s from=%s to=%s delay=%.3fs copy=%s/%s",
                                 sequence, sender, destination, delay, copy + 1, copies)
                    heapq.heappush(pending, (time.monotonic() + delay,
                                            sequence, data, sender, destination))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=int, default=5)
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--base-port", type=int, default=5001)
    parser.add_argument("--max-delay", type=float, default=5.0,
                        help="maximum delivery delay in seconds (default: 5)")
    parser.add_argument("--seed", type=int)
    parser.add_argument("--drop-rate", type=float, default=0.0)
    parser.add_argument("--duplicate-rate", type=float, default=0.0)
    parser.add_argument("--partition-nodes", type=int, nargs="*", default=[],
                        help="agent IDs separated from the rest of the group")
    parser.add_argument("--partition-duration", type=float, default=10.0)
    parser.add_argument("--outage-period", type=float, default=0.0,
                        help="repeat a network-wide outage every N seconds; 0 disables it")
    parser.add_argument("--outage-duration", type=float, default=0.0)
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    args = parser.parse_args()
    if args.nodes < 1 or not math.isfinite(args.max_delay) or args.max_delay < 0:
        parser.error("nodes must be positive and max-delay nonnegative")
    if any(node not in range(args.nodes) for node in args.partition_nodes):
        parser.error("partition-nodes must contain valid agent IDs")
    try:
        issues = NetworkIssues(args.drop_rate, args.duplicate_rate,
                               tuple(args.partition_nodes), args.partition_duration,
                               args.outage_period, args.outage_duration)
    except ValueError as error:
        parser.error(str(error))
    configure_logging(args.log_level)
    try:
        run(args.nodes, args.port, args.base_port, args.max_delay, args.seed, issues)
    except KeyboardInterrupt:
        pass
