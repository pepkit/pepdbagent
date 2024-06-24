import os
import warnings

import pytest
from sqlalchemy.exc import OperationalError

import pepdbagent
from pepdbagent.exceptions import ProjectAlreadyInFavorites, ProjectNotInFavorites

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
