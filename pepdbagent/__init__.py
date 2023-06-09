""" Package-level data """
import coloredlogs
import logmuse

from ._version import __version__
from .pepdbagent import *

_LOGGER = logmuse.init_logger("pepdbagent")
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] %(message)s",
)
