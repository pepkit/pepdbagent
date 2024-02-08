import datetime
import os
import warnings

import peppy
import pytest
from sqlalchemy.exc import OperationalError

import pepdbagent
from pepdbagent.exceptions import (
    FilterError,
    ProjectNotFoundError,
    ProjectNotInFavorites,
    ProjectAlreadyInFavorites,
    SampleNotFoundError,
    ViewNotFoundError,
    SampleAlreadyInView,
    SampleNotInViewError,
)
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
            ["namespace3", "subtable1"],
            # ["namespace2", "derive"],
            # ["namespace2", "imply"],
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
        new_prj = initiate_pepdb_con.project.get(namespace=namespace, name=name)
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
        "namespace, name",
        [
            ["namespace6", "amendments1"],
        ],
    )
    def test_annotation_of_one_non_existing_project(self, initiate_pepdb_con, namespace, name):
        with pytest.raises(ProjectNotFoundError):
            initiate_pepdb_con.annotation.get(
                namespace=namespace,
                name=name,
                tag="default",
            )

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
        assert result.results[0].model_fields_set == {
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
            "pop",
            "stars_number",
            "forked_from",
        }

    @pytest.mark.parametrize(
        "namespace, query, found_number",
        [
            ["namespace1", "ame", 2],
            [None, "re", 3],
        ],
    )
    def test_search_filter_success(self, initiate_pepdb_con, namespace, query, found_number):
        date_now = datetime.datetime.now() + datetime.timedelta(days=1)
        date_old = datetime.datetime.now() - datetime.timedelta(days=5)
        result = initiate_pepdb_con.annotation.get(
            namespace=namespace,
            query=query,
            admin="private_test",
            filter_by="submission_date",
            filter_start_date=date_old.strftime("%Y/%m/%d"),
            filter_end_date=date_now.strftime("%Y/%m/%d"),
        )
        assert len(result.results) == found_number

    @pytest.mark.parametrize(
        "namespace, query, found_number",
        [
            ["namespace1", "ame", 0],
            [None, "re", 0],
        ],
    )
    def test_search_filter_zero_prj(self, initiate_pepdb_con, namespace, query, found_number):
        date_now = datetime.datetime.now() - datetime.timedelta(days=2)
        date_old = date_now - datetime.timedelta(days=2)
        result = initiate_pepdb_con.annotation.get(
            namespace=namespace,
            query=query,
            admin="private_test",
            filter_by="submission_date",
            filter_start_date=date_old.strftime("%Y/%m/%d"),
            filter_end_date=date_now.strftime("%Y/%m/%d"),
        )
        assert len(result.results) == found_number

    @pytest.mark.parametrize(
        "namespace, query, found_number",
        [
            ["namespace1", "ame", 2],
        ],
    )
    def test_search_incorrect_filter_by_string(
        self, initiate_pepdb_con, namespace, query, found_number
    ):
        date_now = datetime.datetime.now() - datetime.timedelta(days=2)
        date_old = date_now - datetime.timedelta(days=2)
        with pytest.raises(FilterError):
            initiate_pepdb_con.annotation.get(
                namespace=namespace,
                query=query,
                admin="private_test",
                filter_by="incorrect",
                filter_start_date=date_old.strftime("%Y/%m/%d"),
                filter_end_date=date_now.strftime("%Y/%m/%d"),
            )

    @pytest.mark.parametrize(
        "rp_list, admin, found_number",
        [
            [
                [
                    "namespace1/amendments1:default",
                    "namespace1/amendments2:default",
                    "namespace2/derive:default",
                    "private_test/amendments1:default",
                ],
                "namespace1",
                4,
            ],
            [
                [
                    "namespace1/amendments1:default",
                    "namespace1/amendments2:default",
                    "namespace2/derive:default",
                    "private_test/amendments1:default",
                ],
                "private_test",
                4,
            ],
        ],
    )
    def test_get_annotation_by_rp_list(self, initiate_pepdb_con, rp_list, admin, found_number):
        result = initiate_pepdb_con.annotation.get_by_rp_list(rp_list)
        assert len(result.results) == found_number

    def test_get_annotation_by_rp_enpty_list(self, initiate_pepdb_con):
        result = initiate_pepdb_con.annotation.get_by_rp_list([])
        assert len(result.results) == 0

    @pytest.mark.parametrize(
        "namespace, query, found_number",
        [
            ["namespace1", "ame", 2],
        ],
    )
    def test_search_incorrect_incorrect_pep_type(
        self, initiate_pepdb_con, namespace, query, found_number
    ):
        with pytest.raises(ValueError):
            initiate_pepdb_con.annotation.get(namespace=namespace, pep_type="incorrect")

    @pytest.mark.parametrize(
        "namespace, query, found_number",
        [
            ["namespace1", "ame", 2],
        ],
    )
    def test_project_list_without_annotation(
        self, initiate_pepdb_con, namespace, query, found_number
    ):
        result = initiate_pepdb_con.annotation.get_projects_list(
            namespace=namespace,
            search_str=query,
        )
        assert len(result) == found_number


@pytest.mark.skipif(
    not db_setup(),
    reason="DB is not setup",
)
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

    def test_namespace_info(self, initiate_pepdb_con):
        initiate_pepdb_con.project.update(
            namespace="private_test",
            name="derive",
            tag="default",
            update_dict={"is_private": False},
        )
        result = initiate_pepdb_con.namespace.info()
        assert len(result.results) == 4
        assert result.results[3].number_of_projects == 1

    def test_namespace_stats(self, initiate_pepdb_con):
        stat_result = initiate_pepdb_con.namespace.stats(monthly=True)
        assert next(iter(stat_result.projects_created.values()), 0) == 30


@pytest.mark.skipif(
    not db_setup(),
    reason="DB is not setup",
)
class TestFavorites:
    """
    Test function within user class
    """

    def test_add_projects_to_favorites(self, initiate_pepdb_con):
        result = initiate_pepdb_con.annotation.get(
            namespace="namespace1",
        )
        for project in result.results:
            initiate_pepdb_con.user.add_project_to_favorites(
                "random_namespace", project.namespace, project.name, "default"
            )
        fav_results = initiate_pepdb_con.user.get_favorites("random_namespace")

        assert fav_results.count == len(result.results)

        # This can fail if the order of the results is different
        assert fav_results.results[0].namespace == result.results[0].namespace

    def test_count_project_none(self, initiate_pepdb_con):
        result = initiate_pepdb_con.user.get_favorites("private_test")
        assert result.count == 0

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
        ],
    )
    def test_count_project_one(self, initiate_pepdb_con, namespace, name):
        initiate_pepdb_con.user.add_project_to_favorites(namespace, namespace, name, "default")
        result = initiate_pepdb_con.user.get_favorites("namespace1")
        assert result.count == 1
        result1 = initiate_pepdb_con.user.get_favorites("private_test")
        assert result1.count == 0

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
        ],
    )
    def test_remove_from_favorite(self, initiate_pepdb_con, namespace, name):
        initiate_pepdb_con.user.add_project_to_favorites("namespace1", namespace, name, "default")
        initiate_pepdb_con.user.add_project_to_favorites(
            "namespace1", namespace, "amendments2", "default"
        )
        result = initiate_pepdb_con.user.get_favorites("namespace1")
        assert result.count == len(result.results) == 2
        initiate_pepdb_con.user.remove_project_from_favorites(
            "namespace1", namespace, name, "default"
        )
        result = initiate_pepdb_con.user.get_favorites("namespace1")
        assert result.count == len(result.results) == 1

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
        ],
    )
    def test_remove_from_favorite_error(self, initiate_pepdb_con, namespace, name):
        with pytest.raises(ProjectNotInFavorites):
            initiate_pepdb_con.user.remove_project_from_favorites(
                "namespace1", namespace, name, "default"
            )

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
        ],
    )
    def test_favorites_duplication_error(self, initiate_pepdb_con, namespace, name):
        initiate_pepdb_con.user.add_project_to_favorites("namespace1", namespace, name, "default")
        with pytest.raises(ProjectAlreadyInFavorites):
            initiate_pepdb_con.user.add_project_to_favorites(
                "namespace1", namespace, name, "default"
            )

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
        ],
    )
    def test_annotation_favorite_number(self, initiate_pepdb_con, namespace, name):
        initiate_pepdb_con.user.add_project_to_favorites("namespace1", namespace, name, "default")
        annotations_in_namespace = initiate_pepdb_con.annotation.get("namespace1")

        for prj_annot in annotations_in_namespace.results:
            if prj_annot.name == name:
                assert prj_annot.stars_number == 1
            else:
                assert prj_annot.stars_number == 0


@pytest.mark.skipif(
    not db_setup(),
    reason="DB is not setup",
)
class TestSamples:
    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_retrieve_one_sample(self, initiate_pepdb_con, namespace, name, sample_name):
        one_sample = initiate_pepdb_con.sample.get(namespace, name, sample_name)
        assert isinstance(one_sample, peppy.Sample)
        assert one_sample.sample_name == sample_name

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_retrieve_raw_sample(self, initiate_pepdb_con, namespace, name, sample_name):
        one_sample = initiate_pepdb_con.sample.get(namespace, name, sample_name, raw=True)
        assert isinstance(one_sample, dict)
        assert one_sample["sample_name"] == sample_name

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace2", "custom_index", "frog_1"],
        ],
    )
    def test_retrieve_sample_with_modified_sample_id(
        self, initiate_pepdb_con, namespace, name, sample_name
    ):
        one_sample = initiate_pepdb_con.sample.get(namespace, name, sample_name)
        assert isinstance(one_sample, peppy.Sample)
        assert one_sample.sample_id == "frog_1"

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_update(self, initiate_pepdb_con, namespace, name, sample_name):
        initiate_pepdb_con.sample.update(
            namespace=namespace,
            name=name,
            tag="default",
            sample_name=sample_name,
            update_dict={"organism": "butterfly"},
        )
        one_sample = initiate_pepdb_con.sample.get(namespace, name, sample_name)
        assert one_sample.organism == "butterfly"

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_update_sample_name(self, initiate_pepdb_con, namespace, name, sample_name):
        initiate_pepdb_con.sample.update(
            namespace=namespace,
            name=name,
            tag="default",
            sample_name=sample_name,
            update_dict={"sample_name": "butterfly"},
        )
        one_sample = initiate_pepdb_con.sample.get(namespace, name, "butterfly")
        assert one_sample.sample_name == "butterfly"

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace2", "custom_index", "frog_1"],
        ],
    )
    def test_update_custom_sample_id(self, initiate_pepdb_con, namespace, name, sample_name):
        initiate_pepdb_con.sample.update(
            namespace=namespace,
            name=name,
            tag="default",
            sample_name=sample_name,
            update_dict={"sample_id": "butterfly"},
        )
        one_sample = initiate_pepdb_con.sample.get(namespace, name, "butterfly")
        assert one_sample.sample_id == "butterfly"

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_add_new_attributes(self, initiate_pepdb_con, namespace, name, sample_name):
        initiate_pepdb_con.sample.update(
            namespace=namespace,
            name=name,
            tag="default",
            sample_name=sample_name,
            update_dict={"new_attr": "butterfly"},
        )
        prj = initiate_pepdb_con.project.get(namespace, name)

        assert prj.get_sample(sample_name).new_attr == "butterfly"

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_project_timestamp_was_changed(self, initiate_pepdb_con, namespace, name, sample_name):
        annotation1 = initiate_pepdb_con.annotation.get(namespace, name, "default")
        import time

        time.sleep(0.2)
        initiate_pepdb_con.sample.update(
            namespace=namespace,
            name=name,
            tag="default",
            sample_name=sample_name,
            update_dict={"new_attr": "butterfly"},
        )
        annotation2 = initiate_pepdb_con.annotation.get(namespace, name, "default")

        assert annotation1.results[0].last_update_date != annotation2.results[0].last_update_date

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_delete_sample(self, initiate_pepdb_con, namespace, name, sample_name):
        one_sample = initiate_pepdb_con.sample.get(namespace, name, sample_name)
        assert isinstance(one_sample, peppy.Sample)

        initiate_pepdb_con.sample.delete(namespace, name, tag="default", sample_name=sample_name)

        with pytest.raises(SampleNotFoundError):
            initiate_pepdb_con.sample.get(namespace, name, tag="default", sample_name=sample_name)

    @pytest.mark.parametrize(
        "namespace, name, tag, sample_dict",
        [
            [
                "namespace1",
                "amendments1",
                "default",
                {
                    "sample_name": "new_sample",
                    "time": "new_time",
                },
            ],
        ],
    )
    def test_add_sample(self, initiate_pepdb_con, namespace, name, tag, sample_dict):
        prj = initiate_pepdb_con.project.get(namespace, name)
        initiate_pepdb_con.sample.add(namespace, name, tag, sample_dict)

        prj2 = initiate_pepdb_con.project.get(namespace, name)

        assert len(prj.samples) + 1 == len(prj2.samples)
        assert prj2.samples[-1].sample_name == sample_dict["sample_name"]

    @pytest.mark.parametrize(
        "namespace, name, tag, sample_dict",
        [
            [
                "namespace1",
                "amendments1",
                "default",
                {
                    "sample_name": "pig_0h",
                    "time": "new_time",
                },
            ],
        ],
    )
    def test_overwrite_sample(self, initiate_pepdb_con, namespace, name, tag, sample_dict):
        assert initiate_pepdb_con.project.get(namespace, name).get_sample("pig_0h").time == "0"
        initiate_pepdb_con.sample.add(namespace, name, tag, sample_dict, overwrite=True)

        assert (
            initiate_pepdb_con.project.get(namespace, name).get_sample("pig_0h").time == "new_time"
        )

    @pytest.mark.parametrize(
        "namespace, name, tag, sample_dict",
        [
            [
                "namespace1",
                "amendments1",
                "default",
                {
                    "sample_name": "new_sample",
                    "time": "new_time",
                },
            ],
        ],
    )
    def test_delete_and_add(self, initiate_pepdb_con, namespace, name, tag, sample_dict):
        prj = initiate_pepdb_con.project.get(namespace, name)
        sample_dict = initiate_pepdb_con.sample.get(namespace, name, "pig_0h", raw=True)
        initiate_pepdb_con.sample.delete(namespace, name, tag, "pig_0h")
        initiate_pepdb_con.sample.add(namespace, name, tag, sample_dict)
        prj2 = initiate_pepdb_con.project.get(namespace, name)
        assert prj.get_sample("pig_0h").to_dict() == prj2.get_sample("pig_0h").to_dict()


@pytest.mark.skipif(
    not db_setup(),
    reason="DB is not setup",
)
class TestViews:
    """
    Test function within view class
    """

    @pytest.mark.parametrize(
        "namespace, name, sample_name, view_name",
        [
            ["namespace1", "amendments1", "pig_0h", "view1"],
        ],
    )
    def test_create_view(self, initiate_pepdb_con, namespace, name, sample_name, view_name):
        initiate_pepdb_con.view.create(
            view_name,
            {
                "project_namespace": namespace,
                "project_name": name,
                "project_tag": "default",
                "sample_list": [sample_name, "pig_1h"],
            },
        )

        project = initiate_pepdb_con.project.get(namespace, name)
        view_project = initiate_pepdb_con.view.get(namespace, name, "default", view_name)
        assert len(view_project.samples) == 2
        assert view_project != project

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_delete_view(self, initiate_pepdb_con, namespace, name, sample_name):
        initiate_pepdb_con.view.create(
            "view1",
            {
                "project_namespace": namespace,
                "project_name": name,
                "project_tag": "default",
                "sample_list": [sample_name, "pig_1h"],
            },
        )
        assert len(initiate_pepdb_con.view.get(namespace, name, "default", "view1").samples) == 2
        initiate_pepdb_con.view.delete(namespace, name, "default", "view1")
        with pytest.raises(ViewNotFoundError):
            initiate_pepdb_con.view.get(namespace, name, "default", "view1")
        assert len(initiate_pepdb_con.project.get(namespace, name).samples) == 4

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_add_sample_to_view(self, initiate_pepdb_con, namespace, name, sample_name):
        initiate_pepdb_con.view.create(
            "view1",
            {
                "project_namespace": namespace,
                "project_name": name,
                "project_tag": "default",
                "sample_list": [sample_name],
            },
        )
        initiate_pepdb_con.view.add_sample(namespace, name, "default", "view1", "pig_1h")
        assert len(initiate_pepdb_con.view.get(namespace, name, "default", "view1").samples) == 2

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_add_multiple_samples_to_view(self, initiate_pepdb_con, namespace, name, sample_name):
        initiate_pepdb_con.view.create(
            "view1",
            {
                "project_namespace": namespace,
                "project_name": name,
                "project_tag": "default",
                "sample_list": [sample_name],
            },
        )
        initiate_pepdb_con.view.add_sample(
            namespace, name, "default", "view1", ["pig_1h", "frog_0h"]
        )
        assert len(initiate_pepdb_con.view.get(namespace, name, "default", "view1").samples) == 3

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_remove_sample_from_view(self, initiate_pepdb_con, namespace, name, sample_name):
        initiate_pepdb_con.view.create(
            "view1",
            {
                "project_namespace": namespace,
                "project_name": name,
                "project_tag": "default",
                "sample_list": [sample_name, "pig_1h"],
            },
        )
        initiate_pepdb_con.view.remove_sample(namespace, name, "default", "view1", sample_name)
        assert len(initiate_pepdb_con.view.get(namespace, name, "default", "view1").samples) == 1
        assert len(initiate_pepdb_con.project.get(namespace, name).samples) == 4

        with pytest.raises(SampleNotInViewError):
            initiate_pepdb_con.view.remove_sample(namespace, name, "default", "view1", sample_name)

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_add_existing_sample_in_view(self, initiate_pepdb_con, namespace, name, sample_name):
        initiate_pepdb_con.view.create(
            "view1",
            {
                "project_namespace": namespace,
                "project_name": name,
                "project_tag": "default",
                "sample_list": [sample_name, "pig_1h"],
            },
        )
        with pytest.raises(SampleAlreadyInView):
            initiate_pepdb_con.view.add_sample(namespace, name, "default", "view1", sample_name)

    @pytest.mark.parametrize(
        "namespace, name, sample_name, view_name",
        [
            ["namespace1", "amendments1", "pig_0h", "view1"],
        ],
    )
    def test_get_snap_view(self, initiate_pepdb_con, namespace, name, sample_name, view_name):
        snap_project = initiate_pepdb_con.view.get_snap_view(
            namespace=namespace,
            name=name,
            tag="default",
            sample_name_list=[sample_name, "pig_1h"],
        )

        assert len(snap_project.samples) == 2

    @pytest.mark.parametrize(
        "namespace, name, sample_name, view_name",
        [
            ["namespace1", "amendments1", "pig_0h", "view1"],
        ],
    )
    def test_get_view_list_from_project(
        self, initiate_pepdb_con, namespace, name, sample_name, view_name
    ):
        assert (
            len(initiate_pepdb_con.view.get_views_annotation(namespace, name, "default").views)
            == 0
        )
        initiate_pepdb_con.view.create(
            "view1",
            {
                "project_namespace": namespace,
                "project_name": name,
                "project_tag": "default",
                "sample_list": [sample_name, "pig_1h"],
            },
        )
        assert (
            len(initiate_pepdb_con.view.get_views_annotation(namespace, name, "default").views)
            == 1
        )
