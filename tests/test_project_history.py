import numpy as np
import peppy
import pytest

from pepdbagent.exceptions import ProjectNotFoundError

from .utils import PEPDBAgentContextManager, get_path_to_example_file, list_of_available_peps


@pytest.mark.skipif(
    not PEPDBAgentContextManager().db_setup(),
    reason="DB is not setup",
)
class TestProjectHistory:
    """
    Test project methods
    """

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_get_update_history(self, namespace, name, sample_name): ...

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_get_insert_history(self, namespace, name, sample_name): ...

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_get_delete_history(self, namespace, name, sample_name): ...

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_get_correct_order(self, namespace, name, sample_name): ...

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_get_history(self, namespace, name, sample_name): ...

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_get_correct_order(self, namespace, name, sample_name): ...
