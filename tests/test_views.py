import os
import warnings

import pytest
from sqlalchemy.exc import OperationalError

import pepdbagent
from pepdbagent.exceptions import (
    SampleAlreadyInView,
    SampleNotFoundError,
    SampleNotInViewError,
    ViewNotFoundError,
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

        project = initiate_pepdb_con.project.get(namespace, name, raw=False)
        view_project = initiate_pepdb_con.view.get(
            namespace, name, "default", view_name, raw=False
        )
        assert len(view_project.samples) == 2
        assert view_project != project

    @pytest.mark.parametrize(
        "namespace, name, sample_name, view_name",
        [
            ["namespace1", "amendments1", "pig_0h", "view1"],
        ],
    )
    def test_create_view_with_incorrect_sample(
        self, initiate_pepdb_con, namespace, name, sample_name, view_name
    ):
        with pytest.raises(SampleNotFoundError):
            initiate_pepdb_con.view.create(
                "view1",
                {
                    "project_namespace": "namespace1",
                    "project_name": "amendments1",
                    "project_tag": "default",
                    "sample_list": ["pig_0h", "pig_1h", "pig_2h"],
                },
            )

    @pytest.mark.parametrize(
        "namespace, name, sample_name, view_name",
        [
            ["namespace1", "amendments1", "pig_0h", "view1"],
        ],
    )
    def test_create_view_with_incorrect_sample_no_fail(
        self, initiate_pepdb_con, namespace, name, sample_name, view_name
    ):
        initiate_pepdb_con.view.create(
            "view1",
            {
                "project_namespace": "namespace1",
                "project_name": "amendments1",
                "project_tag": "default",
                "sample_list": ["pig_0h", "pig_1h", "pig_2h"],
            },
            no_fail=True,
        )
        project = initiate_pepdb_con.project.get(namespace, name, raw=False)
        view_project = initiate_pepdb_con.view.get(
            namespace, name, "default", view_name, raw=False
        )
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
        assert (
            len(
                initiate_pepdb_con.view.get(namespace, name, "default", "view1", raw=False).samples
            )
            == 2
        )
        initiate_pepdb_con.view.delete(namespace, name, "default", "view1")
        with pytest.raises(ViewNotFoundError):
            initiate_pepdb_con.view.get(namespace, name, "default", "view1", raw=False)
        assert len(initiate_pepdb_con.project.get(namespace, name, raw=False).samples) == 4

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
        assert (
            len(
                initiate_pepdb_con.view.get(namespace, name, "default", "view1", raw=False).samples
            )
            == 2
        )

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
        assert (
            len(
                initiate_pepdb_con.view.get(namespace, name, "default", "view1", raw=False).samples
            )
            == 3
        )

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
        assert (
            len(
                initiate_pepdb_con.view.get(namespace, name, "default", "view1", raw=False).samples
            )
            == 1
        )
        assert len(initiate_pepdb_con.project.get(namespace, name, raw=False).samples) == 4

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
