import pytest
import peppy
import os
from pepdbagent.exceptions import ProjectNotFoundError


DNS = f"postgresql://postgres:docker@localhost:5432/pep-db"


DATA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests",
    "data",
)


def get_path_to_example_file(namespace, project_name):
    return os.path.join(DATA_PATH, namespace, project_name, "project_config.yaml")


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
        kk = initiate_pepdb_con.project.get(namespace=namespace, name=name, tag="default")
        ff = peppy.Project(get_path_to_example_file(namespace, name))
        assert kk == ff

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
            kk = initiate_pepdb_con.project.get(namespace=namespace, name=name, tag=tag)

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
        new_prj = initiate_pepdb_con.project.get(namespace="namespace1", name="basic")

        initiate_pepdb_con.project.create(
            project=new_prj,
            namespace=namespace,
            name=name,
            tag="default",
            overwrite=True,
        )

        assert initiate_pepdb_con.project.get(namespace=namespace, name=name) == new_prj

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
            kk = initiate_pepdb_con.project.get(namespace=namespace, name=name, tag="default")


class TestProjectUpdate:
    @pytest.mark.parametrize(
        "namespace, name,new_name",
        [
            ["namespace1", "amendments1", "name1"],
            ["namespace1", "amendments2", "name2"],
            ["namespace2", "derive", "name3"],
            ["namespace1", "basic", "name4"],
            ["namespace2", "derive", "name5"],
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
        "namespace, name, new_tag",
        [
            ["namespace1", "amendments1", "tag1"],
            ["namespace1", "amendments2", "tag2"],
            ["namespace2", "derive", "tag3"],
            ["namespace1", "basic", "tag4"],
            ["namespace2", "derive", "tag5"],
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
            ["namespace1", "amendments2", "desc2 f"],
            ["namespace2", "derive", "desc3 f"],
            ["namespace1", "basic", "desc4 f"],
            ["namespace2", "derive", "desc5 f"],
        ],
    )
    def test_update_project_description(
        self, initiate_pepdb_con, namespace, name, new_description
    ):
        prj = initiate_pepdb_con.project.get(namespace=namespace, name=name)
        prj.description = new_description
        initiate_pepdb_con.project.update(
            namespace=namespace,
            name=name,
            tag="default",
            update_dict={"project": prj},
        )

        assert (
            initiate_pepdb_con.project.get(namespace=namespace, name=name).description
            == new_description
        )

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace1", "amendments2"],
            ["namespace2", "derive"],
            ["namespace2", "imply"],
        ],
    )
    def test_update_whole_project(self, initiate_pepdb_con, namespace, name):
        new_prj = initiate_pepdb_con.project.get(namespace="namespace1", name="basic")
        # update name. If name is different, it will update name too
        new_prj.name = name
        initiate_pepdb_con.project.update(
            namespace=namespace,
            name=name,
            tag="default",
            update_dict={"project": new_prj},
        )

        assert initiate_pepdb_con.project.get(namespace=namespace, name=name) == new_prj

    @pytest.mark.parametrize(
        "namespace, name, pep_schema",
        [
            ["namespace1", "amendments1", "schema1"],
            ["namespace1", "amendments2", "schema2"],
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


class TestAnnotation:
    """
    Test function within annotation class
    """

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace1", "amendments2"],
            ["namespace2", "derive"],
            ["namespace2", "imply"],
            ["namespace3", "subtable1"],
        ],
    )
    def test_annotation_of_one_project(self, initiate_pepdb_con, namespace, name):
        result = initiate_pepdb_con.annotation.get(
            namespace=namespace,
            name=name,
            tag="default",
        )
        assert result.results[0].namespace == namespace

    @pytest.mark.parametrize(
        "namespace, n_projects",
        [
            ["namespace1", 6],
            ["namespace2", 8],
            ["namespace3", 10],
            ["private", 0],
            ["private_test", 0],
        ],
    )
    def test_annotation_all(self, initiate_pepdb_con, namespace, n_projects):
        result = initiate_pepdb_con.annotation.get(
            namespace=namespace,
        )
        assert result.count == n_projects
        assert len(result.results) == n_projects

    @pytest.mark.parametrize(
        "namespace, n_projects",
        [
            ["namespace1", 6],
            ["namespace2", 8],
            ["namespace3", 10],
            ["private", 0],
            ["private_test", 6],
        ],
    )
    @pytest.mark.parametrize("admin", ("private_test", ["private_test", "bbb"]))
    def test_annotation_all_private(self, initiate_pepdb_con, namespace, n_projects, admin):
        result = initiate_pepdb_con.annotation.get(namespace=namespace, admin=admin)
        assert result.count == n_projects
        assert len(result.results) == n_projects

    @pytest.mark.parametrize(
        "namespace, limit, n_projects",
        [
            ["namespace1", 3, 6],
            ["namespace2", 2, 8],
            ["namespace3", 8, 10],
            ["private", 0, 0],
            ["private_test", 5, 6],
        ],
    )
    @pytest.mark.parametrize("admin", ("private_test", ["private_test", "bbb"]))
    def test_annotation_limit(self, initiate_pepdb_con, namespace, limit, admin, n_projects):
        result = initiate_pepdb_con.annotation.get(namespace=namespace, admin=admin, limit=limit)
        assert result.count == n_projects
        assert len(result.results) == limit

    @pytest.mark.parametrize(
        "namespace, order_by, first_name",
        [
            ["namespace1", "name", "amendments1"],
            ["namespace2", "name", "biocproject_exceptions"],
            ["namespace3", "name", "node_alias"],
            ["private_test", "name", "amendments1"],
        ],
    )
    @pytest.mark.parametrize("admin", ["private_test"])
    def test_order_by(self, initiate_pepdb_con, namespace, admin, order_by, first_name):
        result = initiate_pepdb_con.annotation.get(
            namespace=namespace, admin=admin, order_by=order_by
        )
        assert result.results[0].name == first_name

    @pytest.mark.parametrize(
        "namespace, order_by, last_name",
        [
            ["namespace1", "name", "biocproject"],
            ["namespace2", "name", "imports"],
            ["namespace3", "name", "subtables"],
            ["private_test", "name", "subtable3"],
        ],
    )
    @pytest.mark.parametrize("admin", ["private_test"])
    def test_order_by_desc(self, initiate_pepdb_con, namespace, admin, order_by, last_name):
        result = initiate_pepdb_con.annotation.get(
            namespace=namespace,
            admin=admin,
            order_by=order_by,
            order_desc=True,
        )
        assert result.results[0].name == last_name

    @pytest.mark.parametrize(
        "namespace, query, found_number",
        [
            ["namespace1", "ame", 2],
            ["namespace2", "proj", 2],
            ["namespace3", "ABLE", 6],
            ["private_test", "a", 0],
            [None, "re", 2],
        ],
    )
    def test_name_search(self, initiate_pepdb_con, namespace, query, found_number):
        result = initiate_pepdb_con.annotation.get(namespace=namespace, query=query)
        assert len(result.results) == found_number

    @pytest.mark.parametrize(
        "namespace, query, found_number",
        [
            ["namespace1", "ame", 2],
            ["namespace2", "proj", 2],
            ["namespace3", "ABLE", 6],
            ["private_test", "b", 2],
            [None, "re", 3],
        ],
    )
    def test_name_search_private(self, initiate_pepdb_con, namespace, query, found_number):
        result = initiate_pepdb_con.annotation.get(
            namespace=namespace, query=query, admin="private_test"
        )
        assert len(result.results) == found_number

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
            ["namespace1", "amendments2"],
            ["namespace2", "derive"],
            ["namespace2", "imply"],
            ["namespace3", "subtable1"],
        ],
    )
    def test_all_annotations_are_returned(self, initiate_pepdb_con, namespace, name):
        result = initiate_pepdb_con.annotation.get(
            namespace=namespace,
            name=name,
            tag="default",
        )
        assert result.results[0].__fields_set__ == {
            "is_private",
            "tag",
            "namespace",
            "digest",
            "description",
            "number_of_samples",
            "name",
            "last_update_date",
            "submission_date",
            "pep_schema",
        }


class TestNamespace:
    """
    Test function within namespace class
    """

    def test_annotation(self, initiate_pepdb_con):
        result = initiate_pepdb_con.namespace.get()
        assert len(result.results) == 3

    def test_annotation_private(self, initiate_pepdb_con):
        result = initiate_pepdb_con.namespace.get(admin="private_test")
        assert len(result.results) == 4
