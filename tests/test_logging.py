"""Terminal colours remain presentation-only and respect redirection."""
import io
import logging
from unittest.mock import Mock

import pytest

import logging_utils


@pytest.mark.parametrize("level", [logging.DEBUG, logging.INFO, logging.WARNING,
                                   logging.ERROR, logging.CRITICAL, 99])
@pytest.mark.parametrize("use_colour", [False, True])
def test_severity_colours_and_plain_output(level, use_colour):
    record = logging.LogRecord("0", level, "", 0, "value=%r", ("red",), None)
    formatter = logging_utils.ColourFormatter(use_colour)
    output = formatter.format(record)
    assert "0" in output
    assert "value='red'" in output
    assert ("\033[" in output) == use_colour
    assert record.levelname == logging.getLevelName(level)
    assert record.msg == "value=%r"
    assert record.args == ("red",)


@pytest.mark.parametrize("stage,colour", list(logging_utils.STAGE_COLOURS.items()))
def test_each_paxos_stage_is_highlighted(stage, colour):
    record = logging.LogRecord("1", logging.INFO, "", 0,
                               "stage=%s", (stage,), None)
    text = logging_utils.ColourFormatter(True).format(record)
    assert f"stage={colour}{stage}{logging_utils.RESET}" in text


@pytest.mark.parametrize("terminal,no_colour,expected", [
    (True, None, True), (True, "1", False), (True, "", False),
    (False, None, False), (False, "1", False),
])
def test_configuration_colours_only_terminals(monkeypatch, terminal, no_colour, expected):
    stream = io.StringIO()
    monkeypatch.setattr(stream, "isatty", lambda: terminal)
    monkeypatch.setattr(logging_utils.sys, "stderr", stream)
    monkeypatch.delenv("NO_COLOR", raising=False)
    if no_colour is not None:
        monkeypatch.setenv("NO_COLOR", no_colour)
    configure = Mock()
    monkeypatch.setattr(logging_utils.logging, "basicConfig", configure)
    logging_utils.configure_logging("DEBUG")
    options = configure.call_args.kwargs
    assert options["level"] == "DEBUG"
    handler = options["handlers"][0]
    handler.handle(logging.LogRecord("hub", logging.INFO, "", 0, "Ready", (), None))
    assert ("\033[" in stream.getvalue()) == expected
    assert "Ready" in stream.getvalue()
