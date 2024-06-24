import os
import warnings

import numpy as np
import peppy
import pytest
from sqlalchemy.exc import OperationalError

import pepdbagent
from pepdbagent.exceptions import ProjectNotFoundError

from .conftest import DNS

DATA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests",
    "data",
)


def get_path_to_example_file(namespace, project_name):
    return os.path.join(DATA_PATH, namespace, project_name, "project_config.yaml")


def db_setup():
    # Check if the database is setup
    try:
        pepdbagent.PEPDatabaseAgent(dsn=DNS)
    except OperationalError:
        warnings.warn(
            UserWarning(
                f"Skipping tests, because DB is not setup. {DNS}. To setup DB go to README.md"
            )
        )
        return False
    return True


@pytest.mark.skipif(
    not db_setup(),
    reason="DB is not setup",
)
class TestProject:
    """
    Test project methods
    """

    def test_create_project(self, initiate_empty_pepdb_con, list_of_available_peps):
        prj = peppy.Project(list_of_available_peps["namespace3"]["subtables"])
        initiate_empty_pepdb_con.project.create(
            prj, namespace="test", name="imply", overwrite=True
        )
        assert True

    def test_create_project_from_dict(self, initiate_empty_pepdb_con, list_of_available_peps):
        prj = peppy.Project(list_of_available_peps["namespace3"]["subtables"])
        initiate_empty_pepdb_con.project.create(
            prj.to_dict(extended=True, orient="records"),
            namespace="test",
            name="imply",
            overwrite=True,
        )
        assert True

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace1", "amendments2"],
            ["namespace1", "basic"],
            ["namespace2", "derive"],
            ["namespace2", "imply"],
            ["namespace3", "piface"],
            ["namespace3", "subtable2"],
        ],
    )
    def test_get_project(self, initiate_pepdb_con, namespace, name):
        kk = initiate_pepdb_con.project.get(
            namespace=namespace, name=name, tag="default", raw=False
        )
        ff = peppy.Project(get_path_to_example_file(namespace, name))
        assert kk == ff

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
        ],
    )
    def test_get_config(self, initiate_pepdb_con, namespace, name):
        description = ""
        kk = initiate_pepdb_con.project.get_config(
            namespace=namespace,
            name=name,
            tag="default",
        )
        ff = peppy.Project(get_path_to_example_file(namespace, name))
        ff.description = description
        ff.name = name
        assert kk == ff.config

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace3", "subtables"],
        ],
    )
    def test_get_subsamples(self, initiate_pepdb_con, namespace, name):
        prj_subtables = initiate_pepdb_con.project.get_subsamples(
            namespace=namespace,
            name=name,
            tag="default",
        )
        orgiginal_prj = peppy.Project(get_path_to_example_file(namespace, name))

        assert (
            prj_subtables
            == orgiginal_prj.to_dict(extended=True, orient="records")["_subsample_list"]
        )

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace3", "subtables"],
        ],
    )
    def test_get_samples_raw(self, initiate_pepdb_con, namespace, name):
        prj_samples = initiate_pepdb_con.project.get_samples(
            namespace=namespace, name=name, tag="default", raw=True
        )
        orgiginal_prj = peppy.Project(get_path_to_example_file(namespace, name))

        assert (
            prj_samples == orgiginal_prj.to_dict(extended=True, orient="records")["_sample_dict"]
        )

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace3", "subtables"],
        ],
    )
    def test_get_samples_processed(self, initiate_pepdb_con, namespace, name):
        prj_samples = initiate_pepdb_con.project.get_samples(
            namespace=namespace,
            name=name,
            tag="default",
            raw=False,
        )
        orgiginal_prj = peppy.Project(get_path_to_example_file(namespace, name))

        assert prj_samples == orgiginal_prj.sample_table.replace({np.nan: None}).to_dict(
            orient="records"
        )

    @pytest.mark.parametrize(
        "namespace, name,tag",
        [
            ["incorrect_namespace", "amendments1", "default"],
            ["namespace1", "subtable2", "default"],
            ["namespace3", "basic", "default"],
            ["namespace3", "subtable2", "incorrect_tag"],
            ["namespace1", "incorrect_name", "default"],
        ],
    )
    def test_get_project_error(self, initiate_pepdb_con, namespace, name, tag):
        with pytest.raises(ProjectNotFoundError, match="Project does not exist."):
            initiate_pepdb_con.project.get(namespace=namespace, name=name, tag=tag)

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace1", "amendments2"],
            ["namespace2", "derive"],
            ["namespace2", "imply"],
        ],
    )
    def test_overwrite_project(self, initiate_pepdb_con, namespace, name):
        new_prj = initiate_pepdb_con.project.get(namespace="namespace1", name="basic", raw=False)

        initiate_pepdb_con.project.create(
            project=new_prj,
            namespace=namespace,
            name=name,
            tag="default",
            overwrite=True,
        )

        assert initiate_pepdb_con.project.get(namespace=namespace, name=name, raw=False) == new_prj

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace1", "amendments2"],
            ["namespace2", "derive"],
            ["namespace2", "imply"],
        ],
    )
    def test_delete_project(self, initiate_pepdb_con, namespace, name):
        initiate_pepdb_con.project.delete(namespace=namespace, name=name, tag="default")

        with pytest.raises(ProjectNotFoundError, match="Project does not exist."):
            initiate_pepdb_con.project.get(namespace=namespace, name=name, tag="default")

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace1", "amendments2"],
            ["namespace2", "derive"],
            ["namespace2", "imply"],
        ],
    )
    def test_fork_projects(self, initiate_pepdb_con, namespace, name):
        initiate_pepdb_con.project.fork(
            original_namespace=namespace,
            original_name=name,
            original_tag="default",
            fork_namespace="new_namespace",
            fork_name="new_name",
            fork_tag="new_tag",
        )

        assert initiate_pepdb_con.project.exists(
            namespace="new_namespace", name="new_name", tag="new_tag"
        )
        result = initiate_pepdb_con.annotation.get(
            namespace="new_namespace", name="new_name", tag="new_tag"
        )
        assert result.results[0].forked_from == f"{namespace}/{name}:default"

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace1", "amendments2"],
        ],
    )
    def test_parent_project_delete(self, initiate_pepdb_con, namespace, name):
        """
        Test if parent project is deleted, forked project is not deleted
        """
        initiate_pepdb_con.project.fork(
            original_namespace=namespace,
            original_name=name,
            original_tag="default",
            fork_namespace="new_namespace",
            fork_name="new_name",
            fork_tag="new_tag",
        )

        assert initiate_pepdb_con.project.exists(
            namespace="new_namespace", name="new_name", tag="new_tag"
        )
        initiate_pepdb_con.project.delete(namespace=namespace, name=name, tag="default")
        assert initiate_pepdb_con.project.exists(
            namespace="new_namespace", name="new_name", tag="new_tag"
        )

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace1", "amendments2"],
        ],
    )
    def test_child_project_delete(self, initiate_pepdb_con, namespace, name):
        """
        Test if child project is deleted, parent project is not deleted
        """
        initiate_pepdb_con.project.fork(
            original_namespace=namespace,
            original_name=name,
            original_tag="default",
            fork_namespace="new_namespace",
            fork_name="new_name",
            fork_tag="new_tag",
        )

        assert initiate_pepdb_con.project.exists(
            namespace="new_namespace", name="new_name", tag="new_tag"
        )
        assert initiate_pepdb_con.project.exists(namespace=namespace, name=name, tag="default")
        initiate_pepdb_con.project.delete(
            namespace="new_namespace", name="new_name", tag="new_tag"
        )
        assert initiate_pepdb_con.project.exists(namespace=namespace, name=name, tag="default")

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace1", "amendments2"],
        ],
    )
    def test_project_can_be_forked_twice(self, initiate_pepdb_con, namespace, name):
        """
        Test if project can be forked twice
        """
        initiate_pepdb_con.project.fork(
            original_namespace=namespace,
            original_name=name,
            original_tag="default",
            fork_namespace="new_namespace",
            fork_name="new_name",
            fork_tag="new_tag",
        )
        initiate_pepdb_con.project.fork(
            original_namespace=namespace,
            original_name=name,
            original_tag="default",
            fork_namespace="new_namespace2",
            fork_name="new_name2",
            fork_tag="new_tag2",
        )

        result = initiate_pepdb_con.annotation.get(
            namespace="new_namespace", name="new_name", tag="new_tag"
        )
        assert result.results[0].forked_from == f"{namespace}/{name}:default"

        result = initiate_pepdb_con.annotation.get(
            namespace="new_namespace2", name="new_name2", tag="new_tag2"
        )
        assert result.results[0].forked_from == f"{namespace}/{name}:default"
