"""Package-level data."""

from importlib.metadata import version

import coloredlogs
import logmuse

from pepdbagent.pepdbagent import PEPDatabaseAgent

__version__ = version("pepdbagent")

__all__ = ["__version__", "PEPDatabaseAgent"]

_LOGGER = logmuse.init_logger("pepdbagent")
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] %(message)s",
)
