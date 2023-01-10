""" Package-level data """
from .pepdbagent import *
from ._version import __version__

import logmuse
import coloredlogs


_LOGGER = logmuse.init_logger("pepdbagent")
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] %(message)s",
)
