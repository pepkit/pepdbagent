"""Package-level data."""

from importlib.metadata import version

from pepdbagent.pepdbagent import PEPDatabaseAgent

__version__ = version("pepdbagent")

__all__ = ["__version__", "PEPDatabaseAgent"]
