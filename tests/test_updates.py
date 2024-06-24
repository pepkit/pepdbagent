import os
import warnings

import peppy
import pytest
from sqlalchemy.exc import OperationalError

import pepdbagent
from pepdbagent.exceptions import ProjectDuplicatedSampleGUIDsError, SampleTableUpdateError

from .conftest import DNS

DATA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests",
    "data",
)


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
class TestProjectUpdate:
    @pytest.mark.parametrize(
        "namespace, name,new_name",
        [
            ["namespace1", "amendments1", "name1"],
            ["namespace1", "amendments2", "name2"],
        ],
    )
    def test_update_project_name(self, initiate_pepdb_con, namespace, name, new_name):
        initiate_pepdb_con.project.update(
            namespace=namespace,
            name=name,
            tag="default",
            update_dict={"name": new_name},
        )
        assert initiate_pepdb_con.project.exists(namespace=namespace, name=new_name, tag="default")

    @pytest.mark.parametrize(
        "namespace, name,new_name",
        [
            ["namespace1", "amendments1", "name1"],
            ["namespace1", "amendments2", "name2"],
        ],
    )
    def test_update_project_name_in_config(self, initiate_pepdb_con, namespace, name, new_name):
        prj = initiate_pepdb_con.project.get(
            namespace=namespace, name=name, raw=False, with_id=True
        )
        prj.name = new_name
        initiate_pepdb_con.project.update(
            namespace=namespace,
            name=name,
            tag="default",
            update_dict={"project": prj},
        )
        assert initiate_pepdb_con.project.exists(namespace=namespace, name=new_name, tag="default")

    @pytest.mark.parametrize(
        "namespace, name, new_tag",
        [
            ["namespace1", "amendments1", "tag1"],
            ["namespace1", "amendments2", "tag2"],
        ],
    )
    def test_update_project_tag(self, initiate_pepdb_con, namespace, name, new_tag):
        initiate_pepdb_con.project.update(
            namespace=namespace,
            name=name,
            tag="default",
            update_dict={"tag": new_tag},
        )
        assert initiate_pepdb_con.project.exists(namespace=namespace, name=name, tag=new_tag)

    @pytest.mark.parametrize(
        "namespace, name, new_description",
        [
            ["namespace1", "amendments1", "desc1 f"],
            ["namespace2", "derive", "desc5 f"],
        ],
    )
    def test_update_project_description(
        self, initiate_pepdb_con, namespace, name, new_description
    ):
        prj = initiate_pepdb_con.project.get(namespace=namespace, name=name, raw=False)
        prj.description = new_description
        initiate_pepdb_con.project.update(
            namespace=namespace,
            name=name,
            tag="default",
            update_dict={"description": new_description},
        )

        assert (
            initiate_pepdb_con.project.get(namespace=namespace, name=name, raw=False).description
            == new_description
        )

    @pytest.mark.parametrize(
        "namespace, name, new_description",
        [
            ["namespace1", "amendments1", "desc1 f"],
        ],
    )
    def test_update_project_description_in_config(
        self, initiate_pepdb_con, namespace, name, new_description
    ):
        prj = initiate_pepdb_con.project.get(
            namespace=namespace, name=name, raw=False, with_id=True
        )
        prj.description = new_description
        initiate_pepdb_con.project.update(
            namespace=namespace,
            name=name,
            tag="default",
            update_dict={"project": prj},
        )

        assert (
            initiate_pepdb_con.project.get(namespace=namespace, name=name, raw=False).description
            == new_description
        )

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace3", "subtable1"],
        ],
    )
    def test_update_whole_project(self, initiate_pepdb_con, namespace, name):
        new_prj = initiate_pepdb_con.project.get(namespace="namespace1", name="basic", raw=False)
        # update name. If name is different, it will update name too
        new_prj.name = name
        with pytest.raises(SampleTableUpdateError):
            initiate_pepdb_con.project.update(
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
            ["namespace1", "basic", "schema4"],
            ["namespace2", "derive", "schema5"],
        ],
    )
    def test_update_pep_schema(self, initiate_pepdb_con, namespace, name, pep_schema):
        initiate_pepdb_con.project.update(
            namespace=namespace,
            name=name,
            tag="default",
            update_dict={"pep_schema": pep_schema},
        )
        res = initiate_pepdb_con.annotation.get(namespace, name, "default")
        assert res.results[0].pep_schema == pep_schema

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
        ],
    )
    def test_update_project_private(self, initiate_pepdb_con, namespace, name):
        initiate_pepdb_con.project.update(
            namespace=namespace,
            name=name,
            tag="default",
            update_dict={"is_private": True},
        )

        is_private = (
            initiate_pepdb_con.annotation.get(
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
    def test_update_project_pop(self, initiate_pepdb_con, namespace, name):
        initiate_pepdb_con.project.update(
            namespace=namespace,
            name=name,
            tag="default",
            update_dict={"pop": True},
        )

        pop = (
            initiate_pepdb_con.annotation.get(
                namespace=namespace, name=name, tag="default", admin=[namespace]
            )
            .results[0]
            .pop
        )
        assert pop is True

        # Update to pop = False and check if it is updated
        initiate_pepdb_con.project.update(
            namespace=namespace,
            name=name,
            tag="default",
            update_dict={"pop": False},
        )

        pop = (
            initiate_pepdb_con.annotation.get(
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
    def test_project_can_have_2_sample_names(self, initiate_pepdb_con, namespace, name):
        """
        In PEP 2.1.0 project can have 2 rows with same sample name,
        ensure that update works correctly
        """
        new_prj = initiate_pepdb_con.project.get(
            namespace=namespace, name=name, raw=False, with_id=True
        )
        prj_dict = new_prj.to_dict(extended=True, orient="records")

        prj_dict["_sample_dict"].append(
            {"file": "data/frog23_data.txt", "protocol": "anySample3Type", "sample_name": "frog_2"}
        )
        prj_dict["_sample_dict"].append(
            {
                "file": "data/frog23_data.txt4",
                "protocol": "anySample3Type4",
                "sample_name": "frog_2",
            }
        )

        new_prj.description = "new_description"
        initiate_pepdb_con.project.update(
            namespace=namespace,
            name=name,
            tag="default",
            update_dict={"project": peppy.Project.from_dict(prj_dict)},
        )

        prj = initiate_pepdb_con.project.get(namespace=namespace, name=name, raw=True)

        assert len(prj["_sample_dict"]) == 4


@pytest.mark.skipif(
    not db_setup(),
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
    def test_update_whole_project_with_id(self, initiate_pepdb_con, namespace, name):
        pass

    # TODO: write more tests

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            # ["namespace3", "subtable1"],
        ],
    )
    def test_update_project_with_duplicated_sample_guids(
        self, initiate_pepdb_con, namespace, name
    ):
        new_prj = initiate_pepdb_con.project.get(
            namespace=namespace, name=name, raw=True, with_id=True
        )
        new_prj["_sample_dict"].append(new_prj["_sample_dict"][0])

        with pytest.raises(ProjectDuplicatedSampleGUIDsError):
            initiate_pepdb_con.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(new_prj)},
            )


class TestProjectSamplesUpdates:
    ...
    # TODO: write tests
