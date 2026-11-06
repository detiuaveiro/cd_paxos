"""Regression for restarting agents while the same hub still has packets queued."""
import re
import socket
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.udp
def test_persistent_hub_cannot_replay_a_previous_runs_decision(request):
    if not request.config.getoption("--run-udp"):
        pytest.skip("enable live loopback sockets with --run-udp")
    root = Path(__file__).resolve().parents[1]
    for _ in range(100):
        reserved = []
        try:
            probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            reserved.append(probe)
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
            if port > 65530:
                continue
            for offset in range(1, 6):
                probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                reserved.append(probe)
                probe.bind(("127.0.0.1", port + offset))
            break
        except OSError:
            continue
        finally:
            for probe in reserved:
                probe.close()
    else:
        pytest.fail("could not reserve six contiguous UDP ports")

    hub = subprocess.Popen(
        [sys.executable, str(root / "Hub.py"), "--port", str(port), "--base-port", str(port + 1),
         "--max-delay", "0.3", "--seed", "42"],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    try:
        assert "listening" in hub.stderr.readline()
        runs = set()
        for meal in ["hamburger", "pizza", "hamburger", "pizza"]:
            completed = subprocess.run(
                [sys.executable, str(root / "Paxos.py"), "--hub-port", str(port),
                 "--base-port", str(port + 1), "--proposer", "0", "--value", meal],
                capture_output=True, text=True, timeout=15,
            )
            assert completed.returncode == 0, completed.stderr
            assert f"CONSENSUS {meal} (5/5)" in completed.stderr
            decisions = re.findall(r"DECIDED '([^']+)'", completed.stderr)
            assert decisions == [meal] * 5, completed.stderr
            run_id = re.search(r"run=([0-9a-f]{32})", completed.stderr).group(1)
            assert run_id not in runs
            runs.add(run_id)
    finally:
        hub.terminate()
        hub.communicate(timeout=5)
