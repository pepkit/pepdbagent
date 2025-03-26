"""Package-level data"""

import coloredlogs
import logmuse

from pepdbagent._version import __version__
from pepdbagent.pepdbagent import PEPDatabaseAgent

__all__ = ["__version__", "PEPDatabaseAgent"]


_LOGGER = logmuse.init_logger("pepdbagent")
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] %(message)s",
)
