# Basic Paxos over UDP

Distributed Computing assignment — University of Aveiro.

Implement single-decree Paxos so a fixed group of agents agrees on one value.
The example is a dinner decision: agent 0 proposes `pizza` and agent 1 proposes
`hamburger`. Every agent acts as proposer, acceptor and learner.

This repository is a starter project. The protocol methods in `PaxosNode.py`
contain `TODO` markers. Consensus will not complete until they are implemented.

## Learning objectives

- Distinguish the proposer, acceptor and learner roles.
- Implement the two phases of Paxos using JSON messages over UDP.
- Preserve agreement when proposals compete and messages arrive out of order.
- Understand quorum intersection, retries and the limits of progress under faults.

## Provided files

| File | Purpose |
| --- | --- |
| `PaxosNode.py` | Role state, protocol skeleton, retry timers and UDP agent |
| `Paxos.py` | Launcher and competing dinner proposals |
| `Hub.py` | UDP forwarding, delays, loss, duplication and partitions |
| `logging_utils.py` | Terminal logging |
| `tests/` | Automated checks for the protocol and infrastructure |

The socket setup, receive loop, basic timer operations, state dataclasses and
message types are supplied. Backoff is a student exercise. Complete the TODOs in `Paxos.propose()`,
`Paxos.handle()`, `Paxos.decision_message()`, `PaxosNode.broadcast()` and
`PaxosNode.retry_messages()` and `RetryTimer.backoff()`, keeping their public interfaces.
Use `log_state()` and the supplied logger when observing protocol transitions.

## Requirements

A correct solution must:

- generate unique, increasing ballots within the fixed membership;
- implement `PREPARE`, `PROMISE`, `ACCEPT`, `ACCEPTED` and `DECIDE` handling;
- respect promises and retain acceptor state across proposal retries;
- preserve previously accepted values according to the Paxos protocol;
- count distinct members when checking a majority;
- keep votes for different ballots and values separate;
- preserve the first learned decision and propagate valid decisions;
- tolerate duplicate, delayed and reordered messages; and
- allow retries to make progress when a communicating majority is available.

## Message contract

Keep these field names, types and meanings unchanged. Protocol messages are
Python dictionaries serialized as UTF-8 JSON objects, without a nested payload.
Tests may call the protocol directly without using UDP.

| Common field | Type | Requirement |
| --- | --- | --- |
| `type` | `str` | One of `PREPARE`, `PROMISE`, `ACCEPT`, `ACCEPTED`, `DECIDE`; case-sensitive |
| `sender` | `int` | ID of the agent emitting this message, in `0 .. nodes-1` |
| `ballot` | `int` | Nonnegative proposal identifier to which this message refers |

Ballots use `round * nodes + proposer_id`. Rounds are nonnegative integers;
`initial_round` is a lower bound on the first round. Further attempts must exceed
both the agent's previous ballot and the promises it has observed. A ballot in a
reply still identifies the proposal, not the responding agent.

Each message kind requires the common fields plus the following fields:

| `type` | Additional required fields | Meaning |
| --- | --- | --- |
| `PREPARE` | None | Request concerning `ballot` |
| `PROMISE` | `to: int`, `accepted_ballot: int`, `value: str or None` | `to` is the requesting proposer; `accepted_ballot` and `value` describe the acceptor's previously accepted proposal |
| `ACCEPT` | `value: str` | Value requested for this ballot |
| `ACCEPTED` | `value: str` | Value accepted by `sender` for this ballot |
| `DECIDE` | `value: str`, `acceptors: list[int]` | Decision announcement, with IDs of the acceptors supporting this ballot/value pair |

In a `PROMISE`, no previous acceptance is represented by
`accepted_ballot=-1` and `value=None` (JSON `null`); both fields must still be
present. Otherwise `accepted_ballot` is nonnegative and `value` is a string.
An empty string is a valid value. Do not use truthiness to distinguish it from
an absent value. The `-1` sentinel in initial state is not a proposal ballot or
an acceptance notification to send.

`DECIDE.acceptors` must identify at least `nodes // 2 + 1` distinct valid
members. Repeated IDs do not add votes. Outgoing announcements use a sorted list
of distinct IDs; receivers must not rely on incoming list order. The `sender`
of a decision announcement is the announcing learner, which need not be one
of the listed acceptors.

`to` is a logical destination, not a socket address. Messages carrying a `to`
for another agent must be ignored. Messages without `to` are broadcast.
Unknown sender IDs and unsupported message kinds produce no protocol response.
Use only the fields required for each kind in protocol output; do not add
transport metadata in `propose()`, `handle()` or `decision_message()`.

For example, an acceptor with no previously accepted value can emit:

```json
{"type": "PROMISE", "sender": 2, "ballot": 4, "to": 1, "accepted_ballot": -1, "value": null}
```

### Protocol and transport interfaces

- `propose(value)` and `handle(message)` return `list[Message]`; no outgoing
  messages is `[]`, not `None`. `propose` rejects non-string values with
  `ValueError`.
- `decision_message()` returns one `DECIDE` dictionary for an already learned
  decision. It must not change the decision or its supporting ballot.
- `broadcast(messages)` sends through the hub and provides local delivery,
  including any resulting messages. The hub does not echo to the sending agent.
  Targeted messages still obey `to`. Do not mutate the input dictionaries.
- On the wire, `broadcast` adds `run_id: str` to a copy when the node has a run
  ID configured. The supplied receive loop checks it. Direct protocol tests and
  local delivery do not require `run_id`.
- `retry_messages(now)` uses the supplied timers and monotonic time, updates
  `finished` when a decision is learned, and supports proposal and notification
  retries. Notification retries must remain possible after local completion.

Preserve the state attributes and method signatures in the starter: the launcher,
logging and tests inspect them. Keep role stages compatible with the supplied
interface: proposer `IDLE`, `PHASE_1_PREPARE`, `PHASE_2_ACCEPT`; acceptor `READY`,
`PHASE_1_PROMISED`, `PHASE_2_ACCEPTED`; learner `LEARNING`, `DECIDED`.
Use the existing logging helpers to make transitions observable.

## Backoff requirements

Implement `RetryTimer.backoff(now)` without sleeping or reading the system clock.
The caller supplies monotonic time in seconds. Preserve the existing fields and
method signature so retries can be tested using virtual time.

- Each call doubles `delay`, up to the configured `maximum`.
- Select a waiting interval uniformly between 80% and 100% of that updated
  `delay`, using the instance's seeded random generator (`rng`).
- Set `deadline` relative to the supplied `now` and return the selected interval
  as a float. `delay` retains the capped interval before jitter is applied.
- Keep `reset(now)` restoring `delay` to `initial` and scheduling the first
  deadline without jitter. `cancel()` disables the deadline, and `ready(now)`
  becomes true when the deadline is reached, including equality.

For example, with `initial=10` and `maximum=40`, successive backoff calls have
waiting intervals in `[16, 20]`, `[32, 40]`, then `[32, 40]` again. A reset starts
a new sequence. Use `tests/test_retry.py::test_backoff_doubles_caps_and_resets`
to check your implementation; it is expected to fail until this TODO is complete.

## Running

Use Python 3.9 or later. No runtime dependencies are required.

Start the hub in one terminal:

```sh
python Hub.py
```

Start the agents in another:

```sh
python Paxos.py
```

After completing the protocol, all agents should learn the same dinner choice.
The starter remains running without reaching consensus; press Ctrl+C to stop it.
For a single proposer:

```sh
python Paxos.py --proposer 0 --value "Hello Paxos"
```

Use `--log-level DEBUG` to inspect messages and state. Both programs provide
`--help` for all options. Keep node counts and ports consistent between them.
Stop the previous agents before restarting on the same ports.

## Fault experiments

Restart the hub with selected fault options, then launch the agents again:

```sh
python Hub.py --seed 7 --max-delay 1 --duplicate-rate 0.3
python Hub.py --seed 7 --max-delay 1 --drop-rate 0.2
python Hub.py --nodes 3 --partition-nodes 2 --partition-duration 10
```

For the three-node experiment, launch `python Paxos.py --nodes 3`.
Use `--retry-timeout` and `--retry-max` on the launcher to adjust retry delays.
Agreement must hold even when progress is temporarily impossible. A permanent
loss of a majority can prevent a decision; arbitrary message loss does not
provide a bounded completion time.

## Checking your work

```sh
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m pytest
python -m mypy
```

Protocol tests are expected to fail in the starter. Use their failures to check
your implementation. Launcher and logging tests can be run separately:

```sh
python -m pytest tests/test_launcher.py tests/test_logging.py
```

The optional live UDP checks require local socket access:

```sh
python -m pytest --run-udp
```

## Scope

This is a classroom implementation with fixed membership, in-memory state and
trusted messages. It decides a single value, without a replicated log, durable
storage, membership changes or Byzantine fault tolerance.

## GitHub Actions

Workflows use the same `self-hosted` runner model as the Chord assignment.
Evaluation installs development dependencies in `.ci-venv`, runs the full test
suite (including live UDP checks), checks types and saves the test report.
Tests manage their own agents and hub; no background demo is started by CI.
The student skeleton is expected to fail evaluation until the TODOs are complete.

Assignment checks validate `NMEC.txt` (two five-digit student numbers separated
by a hyphen) and submit Python source to `https://moss.av.it.pt/guiao4/upload`.
A daily/manual workflow compares test checksums with the same service's
`/md5sum` endpoint. The solved repository additionally publishes reference
checksums on pushes to its default branch; it permits an absent `NMEC.txt`.
The runner needs Python 3.14 support, loopback UDP, `curl`, `zip` and `md5sum`,
and access to the MOSS service. Workflows become active when pushed to GitHub.
