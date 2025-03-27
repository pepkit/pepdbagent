import peppy
import pytest
from peppy.exceptions import IllegalStateException

from pepdbagent.const import PEPHUB_SAMPLE_ID_KEY
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
        "namespace, name, new_schema",
        [
            ["namespace1", "amendments1", "bedboss"],
            ["namespace2", "derive", "bedboss"],
        ],
    )
    def test_update_project_schema(self, namespace, name, new_schema):
        with PEPDBAgentContextManager(add_data=True) as agent:
            prj_annot = agent.annotation.get(namespace=namespace, name=name)
            assert prj_annot.results[0].pep_schema == "namespace1/2.0.0:1.0.0"

            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"pep_schema": "namespace2/bedboss:1.0.0"},
            )
            prj_annot = agent.annotation.get(namespace=namespace, name=name)
            assert prj_annot.results[0].pep_schema == "namespace2/bedboss:1.0.0"

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
            ["namespace1", "amendments1", "namespace2/bedmaker:1.0.0"],
            ["namespace2", "derive", "namespace2/bedbuncher:1.0.0"],
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
        """
        General test for updating whole project with id (inserting one project without id)
        """
        with PEPDBAgentContextManager(add_data=True) as agent:
            peppy_prj = agent.project.get(namespace=namespace, name=name, raw=True)
            prj = agent.project.get(namespace=namespace, name=name, raw=True, with_id=True)

            new_sample = {
                "sample_name": "new_sample",
                "protocol": "new_protocol",
                PEPHUB_SAMPLE_ID_KEY: None,
            }

            prj["_sample_dict"].append(new_sample.copy())
            prj["_sample_dict"][0]["sample_name"] = "new_sample_name2"
            del prj["_sample_dict"][1]

            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(prj)},
            )

            del new_sample[PEPHUB_SAMPLE_ID_KEY]
            peppy_prj["_sample_dict"].append(new_sample.copy())  # add sample without id
            peppy_prj["_sample_dict"][0]["sample_name"] = "new_sample_name2"  # modify sample
            del peppy_prj["_sample_dict"][1]  # delete sample

            assert peppy.Project.from_dict(peppy_prj) == agent.project.get(
                namespace=namespace, name=name, raw=False
            )

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace3", "subtable1"],
        ],
    )
    def test_insert_new_row(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            peppy_prj = agent.project.get(namespace=namespace, name=name, raw=True)
            prj = agent.project.get(namespace=namespace, name=name, raw=True, with_id=True)

            new_sample = {
                "sample_name": "new_sample",
                "protocol": "new_protocol",
                PEPHUB_SAMPLE_ID_KEY: None,
            }

            prj["_sample_dict"].append(new_sample.copy())

            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(prj)},
            )

            del new_sample[PEPHUB_SAMPLE_ID_KEY]
            peppy_prj["_sample_dict"].append(new_sample.copy())  # add sample without id

            assert peppy.Project.from_dict(peppy_prj) == agent.project.get(
                namespace=namespace, name=name, raw=False
            )

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace3", "subtable1"],
        ],
    )
    def test_insert_new_multiple_rows(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            peppy_prj = agent.project.get(namespace=namespace, name=name, raw=True)
            prj = agent.project.get(namespace=namespace, name=name, raw=True, with_id=True)

            new_sample1 = {
                "sample_name": "new_sample",
                "protocol": "new_protocol",
                PEPHUB_SAMPLE_ID_KEY: None,
            }
            new_sample2 = {
                "sample_name": "new_sample2",
                "protocol": "new_protocol2",
                PEPHUB_SAMPLE_ID_KEY: None,
            }

            prj["_sample_dict"].append(new_sample1.copy())
            prj["_sample_dict"].append(new_sample2.copy())

            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(prj)},
            )

            del new_sample1[PEPHUB_SAMPLE_ID_KEY]
            del new_sample2[PEPHUB_SAMPLE_ID_KEY]
            peppy_prj["_sample_dict"].append(new_sample1.copy())  # add sample without id
            peppy_prj["_sample_dict"].append(new_sample2.copy())  # add sample without id

            assert peppy.Project.from_dict(peppy_prj) == agent.project.get(
                namespace=namespace, name=name, raw=False
            )

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace3", "subtable1"],
        ],
    )
    def test_insert_new_multiple_rows_duplicated_samples(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            prj = agent.project.get(namespace=namespace, name=name, raw=True, with_id=True)

            new_sample1 = {
                "sample_name": "new_sample",
                "protocol": "new_protocol",
                PEPHUB_SAMPLE_ID_KEY: None,
            }
            new_sample2 = {
                "sample_name": "new_sample",
                "protocol": "new_protocol",
                PEPHUB_SAMPLE_ID_KEY: None,
            }

            prj["_sample_dict"].append(new_sample1.copy())
            prj["_sample_dict"].append(new_sample2.copy())

            with pytest.raises(IllegalStateException):
                agent.project.update(
                    namespace=namespace,
                    name=name,
                    tag="default",
                    update_dict={"project": peppy.Project.from_dict(prj)},
                )

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace3", "subtable2"],
            ["namespace1", "append"],
        ],
    )
    def test_delete_multiple_rows(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            peppy_prj = agent.project.get(namespace=namespace, name=name, raw=True)
            prj = agent.project.get(namespace=namespace, name=name, raw=True, with_id=True)

            del prj["_sample_dict"][1]
            del prj["_sample_dict"][2]

            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(prj)},
            )

            del peppy_prj["_sample_dict"][1]  # delete sample
            del peppy_prj["_sample_dict"][2]  # delete sample

            assert peppy.Project.from_dict(peppy_prj) == agent.project.get(
                namespace=namespace, name=name, raw=False
            )

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace3", "subtable1"],
        ],
    )
    def test_modify_one_row(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            peppy_prj = agent.project.get(namespace=namespace, name=name, raw=True)
            prj = agent.project.get(namespace=namespace, name=name, raw=True, with_id=True)

            prj["_sample_dict"][0]["sample_name"] = "new_sample_name2"

            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(prj)},
            )

            peppy_prj["_sample_dict"][0]["sample_name"] = "new_sample_name2"  # modify sample

            assert peppy.Project.from_dict(peppy_prj) == agent.project.get(
                namespace=namespace, name=name, raw=False
            )

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace3", "subtable1"],
        ],
    )
    def test_modify_multiple_rows(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            peppy_prj = agent.project.get(namespace=namespace, name=name, raw=True)
            prj = agent.project.get(namespace=namespace, name=name, raw=True, with_id=True)

            prj["_sample_dict"][0]["sample_name"] = "new_sample_name2"
            prj["_sample_dict"][1]["sample_name"] = "new_sample_name3"

            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(prj)},
            )

            peppy_prj["_sample_dict"][0]["sample_name"] = "new_sample_name2"  # modify sample
            peppy_prj["_sample_dict"][1]["sample_name"] = "new_sample_name3"  # modify sample

            assert peppy.Project.from_dict(peppy_prj) == agent.project.get(
                namespace=namespace, name=name, raw=False
            )

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace3", "subtable1"],
        ],
    )
    def test_add_new_first_sample(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            peppy_prj = agent.project.get(namespace=namespace, name=name, raw=True)
            prj = agent.project.get(namespace=namespace, name=name, raw=True, with_id=True)

            new_sample = {
                "sample_name": "new_sample",
                "protocol": "new_protocol",
                PEPHUB_SAMPLE_ID_KEY: None,
            }

            prj["_sample_dict"].insert(0, new_sample.copy())

            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(prj)},
            )

            del new_sample[PEPHUB_SAMPLE_ID_KEY]
            peppy_prj["_sample_dict"].insert(0, new_sample.copy())  # add sample without id

            assert peppy.Project.from_dict(peppy_prj) == agent.project.get(
                namespace=namespace, name=name, raw=False
            )

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace3", "subtable1"],
        ],
    )
    def test_change_sample_order(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            peppy_prj = agent.project.get(namespace=namespace, name=name, raw=True)
            prj = agent.project.get(namespace=namespace, name=name, raw=True, with_id=True)

            sample1 = prj["_sample_dict"][0].copy()
            sample2 = prj["_sample_dict"][1].copy()

            prj["_sample_dict"][0] = sample2
            prj["_sample_dict"][1] = sample1

            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(prj)},
            )

            peppy_prj["_sample_dict"][0] = sample2
            peppy_prj["_sample_dict"][1] = sample1

            del peppy_prj["_sample_dict"][0][PEPHUB_SAMPLE_ID_KEY]
            del peppy_prj["_sample_dict"][1][PEPHUB_SAMPLE_ID_KEY]

            assert peppy.Project.from_dict(peppy_prj) == agent.project.get(
                namespace=namespace, name=name, raw=False
            )

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace3", "subtable1"],
        ],
    )
    def test_update_porject_without_ids(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            prj = agent.project.get(namespace=namespace, name=name, raw=True, with_id=False)

            prj["_sample_dict"][0]["sample_name"] = "new_sample_name2"

            with pytest.raises(SampleTableUpdateError):

                agent.project.update(
                    namespace=namespace,
                    name=name,
                    tag="default",
                    update_dict={"project": peppy.Project.from_dict(prj)},
                )

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
