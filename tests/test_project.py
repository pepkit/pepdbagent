import numpy as np
import peppy
import pytest

from pepdbagent.exceptions import ProjectNotFoundError

from .utils import PEPDBAgentContextManager, get_path_to_example_file, list_of_available_peps


@pytest.mark.skipif(
    not PEPDBAgentContextManager().db_setup(),
    reason="DB is not setup",
)
class TestProject:
    """
    Test project methods
    """

    def test_create_project(self):
        with PEPDBAgentContextManager(add_data=False) as agent:
            prj = peppy.Project(list_of_available_peps()["namespace3"]["subtables"])
            agent.project.create(prj, namespace="test", name="imply", overwrite=False)
            assert True

    def test_create_project_from_dict(self):
        with PEPDBAgentContextManager(add_data=False) as agent:
            prj = peppy.Project(list_of_available_peps()["namespace3"]["subtables"])
            agent.project.create(
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
    def test_get_project(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            kk = agent.project.get(namespace=namespace, name=name, tag="default", raw=False)
            ff = peppy.Project(get_path_to_example_file(namespace, name))
            assert kk == ff

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
        ],
    )
    def test_get_config(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            description = ""
            kk = agent.project.get_config(
                namespace=namespace,
                name=name,
                tag="default",
            )
            ff = peppy.Project(get_path_to_example_file(namespace, name))
            ff["_original_config"]["description"] = description
            ff["_original_config"]["name"] = name
            assert kk == ff["_original_config"]

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace3", "subtables"],
        ],
    )
    def test_get_subsamples(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            prj_subtables = agent.project.get_subsamples(
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
    def test_get_samples_raw(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            prj_samples = agent.project.get_samples(
                namespace=namespace, name=name, tag="default", raw=True
            )
            orgiginal_prj = peppy.Project(get_path_to_example_file(namespace, name))

            assert (
                prj_samples
                == orgiginal_prj.to_dict(extended=True, orient="records")["_sample_dict"]
            )

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace3", "subtables"],
        ],
    )
    def test_get_samples_processed(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            prj_samples = agent.project.get_samples(
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
    def test_get_project_error(self, namespace, name, tag):
        with PEPDBAgentContextManager(add_data=True) as agent:
            with pytest.raises(ProjectNotFoundError, match="Project does not exist."):
                agent.project.get(namespace=namespace, name=name, tag=tag)

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace1", "amendments2"],
            ["namespace2", "derive"],
            ["namespace2", "imply"],
        ],
    )
    def test_overwrite_project(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            new_prj = agent.project.get(namespace="namespace1", name="basic", raw=False)

            agent.project.create(
                project=new_prj,
                namespace=namespace,
                name=name,
                tag="default",
                overwrite=True,
            )

            assert agent.project.get(namespace=namespace, name=name, raw=False) == new_prj

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace2", "derive"],
        ],
    )
    def test_delete_project(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.project.delete(namespace=namespace, name=name, tag="default")

            with pytest.raises(ProjectNotFoundError, match="Project does not exist."):
                agent.project.get(namespace=namespace, name=name, tag="default")

    def test_delete_not_existing_project(self):
        with PEPDBAgentContextManager(add_data=True) as agent:
            with pytest.raises(ProjectNotFoundError, match="Project does not exist."):
                agent.project.delete(namespace="namespace1", name="nothing", tag="default")

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments2"],
            ["namespace2", "derive"],
            ["namespace2", "imply"],
        ],
    )
    def test_fork_projects(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.project.fork(
                original_namespace=namespace,
                original_name=name,
                original_tag="default",
                fork_namespace="new_namespace",
                fork_name="new_name",
                fork_tag="new_tag",
            )

            assert agent.project.exists(namespace="new_namespace", name="new_name", tag="new_tag")
            result = agent.annotation.get(
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
    def test_parent_project_delete(self, namespace, name):
        """
        Test if parent project is deleted, forked project is not deleted
        """
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.project.fork(
                original_namespace=namespace,
                original_name=name,
                original_tag="default",
                fork_namespace="new_namespace",
                fork_name="new_name",
                fork_tag="new_tag",
            )

            assert agent.project.exists(namespace="new_namespace", name="new_name", tag="new_tag")
            agent.project.delete(namespace=namespace, name=name, tag="default")
            assert agent.project.exists(namespace="new_namespace", name="new_name", tag="new_tag")

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace1", "amendments2"],
        ],
    )
    def test_child_project_delete(self, namespace, name):
        """
        Test if child project is deleted, parent project is not deleted
        """
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.project.fork(
                original_namespace=namespace,
                original_name=name,
                original_tag="default",
                fork_namespace="new_namespace",
                fork_name="new_name",
                fork_tag="new_tag",
            )

            assert agent.project.exists(namespace="new_namespace", name="new_name", tag="new_tag")
            assert agent.project.exists(namespace=namespace, name=name, tag="default")
            agent.project.delete(namespace="new_namespace", name="new_name", tag="new_tag")
            assert agent.project.exists(namespace=namespace, name=name, tag="default")

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace1", "amendments2"],
        ],
    )
    def test_project_can_be_forked_twice(self, namespace, name):
        """
        Test if project can be forked twice
        """
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.project.fork(
                original_namespace=namespace,
                original_name=name,
                original_tag="default",
                fork_namespace="new_namespace",
                fork_name="new_name",
                fork_tag="new_tag",
            )
            agent.project.fork(
                original_namespace=namespace,
                original_name=name,
                original_tag="default",
                fork_namespace="new_namespace2",
                fork_name="new_name2",
                fork_tag="new_tag2",
            )

            result = agent.annotation.get(
                namespace="new_namespace", name="new_name", tag="new_tag"
            )
            assert result.results[0].forked_from == f"{namespace}/{name}:default"

            result = agent.annotation.get(
                namespace="new_namespace2", name="new_name2", tag="new_tag2"
            )
            assert result.results[0].forked_from == f"{namespace}/{name}:default"
