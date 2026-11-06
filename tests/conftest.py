"""Optional live UDP checks complement the deterministic default suite."""


def pytest_addoption(parser):
    parser.addoption("--run-udp", action="store_true", help="run tests using real loopback UDP sockets")
