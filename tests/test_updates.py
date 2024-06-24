import peppy
import pytest
from pepdbagent.exceptions import ProjectDuplicatedSampleGUIDsError, SampleTableUpdateError

from .utils import PEPDBAgentContextManager


@pytest.mark.skipif(
    not PEPDBAgentContextManager().db_setup(),
    reason="DB is not setup",
)
class TestProjectUpdate:
    @pytest.mark.parametrize(
        "namespace, name,new_name",
        [
            ["namespace1", "amendments1", "name1"],
            ["namespace1", "amendments2", "name2"],
        ],
    )
    def test_update_project_name(self, namespace, name, new_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"name": new_name},
            )
            assert agent.project.exists(namespace=namespace, name=new_name, tag="default")

    @pytest.mark.parametrize(
        "namespace, name,new_name",
        [
            ["namespace1", "amendments1", "name1"],
            ["namespace1", "amendments2", "name2"],
        ],
    )
    def test_update_project_name_in_config(self, namespace, name, new_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            prj = agent.project.get(namespace=namespace, name=name, raw=False, with_id=True)
            prj.name = new_name
            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": prj},
            )
            assert agent.project.exists(namespace=namespace, name=new_name, tag="default")

    @pytest.mark.parametrize(
        "namespace, name, new_tag",
        [
            ["namespace1", "amendments1", "tag1"],
            ["namespace1", "amendments2", "tag2"],
        ],
    )
    def test_update_project_tag(self, namespace, name, new_tag):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"tag": new_tag},
            )
            assert agent.project.exists(namespace=namespace, name=name, tag=new_tag)

    @pytest.mark.parametrize(
        "namespace, name, new_description",
        [
            ["namespace1", "amendments1", "desc1 f"],
            ["namespace2", "derive", "desc5 f"],
        ],
    )
    def test_update_project_description(self, namespace, name, new_description):
        with PEPDBAgentContextManager(add_data=True) as agent:
            prj = agent.project.get(namespace=namespace, name=name, raw=False)
            prj.description = new_description
            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"description": new_description},
            )

            assert (
                agent.project.get(namespace=namespace, name=name, raw=False).description
                == new_description
            )

    @pytest.mark.parametrize(
        "namespace, name, new_description",
        [
            ["namespace1", "amendments1", "desc1 f"],
        ],
    )
    def test_update_project_description_in_config(self, namespace, name, new_description):
        with PEPDBAgentContextManager(add_data=True) as agent:
            prj = agent.project.get(namespace=namespace, name=name, raw=False, with_id=True)
            prj.description = new_description
            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": prj},
            )

            assert (
                agent.project.get(namespace=namespace, name=name, raw=False).description
                == new_description
            )

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace3", "subtable1"],
        ],
    )
    def test_update_whole_project(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            new_prj = agent.project.get(namespace="namespace1", name="basic", raw=False)
            # update name. If name is different, it will update name too
            new_prj.name = name
            with pytest.raises(SampleTableUpdateError):
                agent.project.update(
                    namespace=namespace,
                    name=name,
                    tag="default",
                    update_dict={"project": new_prj},
                )

    @pytest.mark.parametrize(
        "namespace, name, pep_schema",
        [
            ["namespace1", "amendments1", "schema1"],
            ["namespace2", "derive", "schema3"],
        ],
    )
    def test_update_pep_schema(self, namespace, name, pep_schema):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"pep_schema": pep_schema},
            )
            res = agent.annotation.get(namespace, name, "default")
            assert res.results[0].pep_schema == pep_schema

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
        ],
    )
    def test_update_project_private(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"is_private": True},
            )

            is_private = (
                agent.annotation.get(
                    namespace=namespace, name=name, tag="default", admin=[namespace]
                )
                .results[0]
                .is_private
            )
            assert is_private is True

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
        ],
    )
    def test_update_project_pop(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"pop": True},
            )

            pop = (
                agent.annotation.get(
                    namespace=namespace, name=name, tag="default", admin=[namespace]
                )
                .results[0]
                .pop
            )
            assert pop is True

            # Update to pop = False and check if it is updated
            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"pop": False},
            )

            pop = (
                agent.annotation.get(
                    namespace=namespace, name=name, tag="default", admin=[namespace]
                )
                .results[0]
                .pop
            )
            assert pop is False

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "basic"],
        ],
    )
    def test_project_can_have_2_sample_names(self, namespace, name):
        """
        In PEP 2.1.0 project can have 2 rows with same sample name,
        ensure that update works correctly
        """
        with PEPDBAgentContextManager(add_data=True) as agent:
            new_prj = agent.project.get(namespace=namespace, name=name, raw=False, with_id=True)
            prj_dict = new_prj.to_dict(extended=True, orient="records")

            prj_dict["_sample_dict"].append(
                {
                    "file": "data/frog23_data.txt",
                    "protocol": "anySample3Type",
                    "sample_name": "frog_2",
                }
            )
            prj_dict["_sample_dict"].append(
                {
                    "file": "data/frog23_data.txt4",
                    "protocol": "anySample3Type4",
                    "sample_name": "frog_2",
                }
            )

            new_prj.description = "new_description"
            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(prj_dict)},
            )

            prj = agent.project.get(namespace=namespace, name=name, raw=True)

            assert len(prj["_sample_dict"]) == 4


@pytest.mark.skipif(
    not PEPDBAgentContextManager().db_setup(),
    reason="DB is not setup",
)
class TestUpdateProjectWithId:
    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace3", "subtable1"],
        ],
    )
    def test_update_whole_project_with_id(self, namespace, name):
        pass

    # TODO: write more tests

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
        ],
    )
    def test_update_project_with_duplicated_sample_guids(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            new_prj = agent.project.get(namespace=namespace, name=name, raw=True, with_id=True)
            new_prj["_sample_dict"].append(new_prj["_sample_dict"][0])

            with pytest.raises(ProjectDuplicatedSampleGUIDsError):
                agent.project.update(
                    namespace=namespace,
                    name=name,
                    tag="default",
                    update_dict={"project": peppy.Project.from_dict(new_prj)},
                )
