"""Shared, dependency-free terminal colours for the classroom examples."""
import logging
import os
import re
import sys


LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s %(message)s"
RESET = "\033[0m"
LEVEL_COLOURS = {
    logging.DEBUG: "\033[36m",     # cyan
    logging.INFO: "\033[32m",      # green
    logging.WARNING: "\033[33m",   # yellow
    logging.ERROR: "\033[31m",     # red
    logging.CRITICAL: "\033[1;31m",
}
STAGE_COLOURS = {
    "PHASE_1_PREPARE": "\033[36m",
    "PHASE_1_PROMISED": "\033[36m",
    "PHASE_2_ACCEPTED": "\033[35m",
    "PHASE_2_ACCEPT": "\033[35m",
    "LEARNING": "\033[33m",
    "DECIDED": "\033[1;32m",
}

STAGE_PATTERN = re.compile(r"\b(" + "|".join(STAGE_COLOURS) + r")\b")


class ColourFormatter(logging.Formatter):
    def __init__(self, use_colour: bool) -> None:
        super().__init__(LOG_FORMAT, datefmt="%H:%M:%S")
        self.use_colour = use_colour

    def format(self, record: logging.LogRecord) -> str:
        text = super().format(record)
        if self.use_colour:
            colour = LEVEL_COLOURS.get(record.levelno, "\033[37m")
            text = text.replace(record.levelname, f"{colour}{record.levelname}{RESET}", 1)
            text = STAGE_PATTERN.sub(
                lambda match: f"{STAGE_COLOURS[match[0]]}{match[0]}{RESET}", text,
            )
        return text


def configure_logging(level: str) -> None:
    """Colour stderr only on terminals; NO_COLOR disables ANSI sequences."""
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(ColourFormatter(sys.stderr.isatty() and "NO_COLOR" not in os.environ))
    logging.basicConfig(level=level, format=LOG_FORMAT, datefmt="%H:%M:%S",
                        handlers=[handler])
