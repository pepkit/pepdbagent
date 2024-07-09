import pytest

from .utils import PEPDBAgentContextManager


@pytest.mark.skipif(
    not PEPDBAgentContextManager().db_setup(),
    reason="DB is not setup",
)
class TestSamples: ...
