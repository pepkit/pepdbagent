import pytest

from pepdbagent.exceptions import (
    SampleAlreadyInView,
    SampleNotFoundError,
    SampleNotInViewError,
    ViewNotFoundError,
)

from .utils import PEPDBAgentContextManager


@pytest.mark.skipif(
    not PEPDBAgentContextManager().db_setup(),
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
    def test_create_view(self, namespace, name, sample_name, view_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.view.create(
                view_name,
                {
                    "project_namespace": namespace,
                    "project_name": name,
                    "project_tag": "default",
                    "sample_list": [sample_name, "pig_1h"],
                },
            )

            project = agent.project.get(namespace, name, raw=False)
            view_project = agent.view.get(namespace, name, "default", view_name, raw=False)
            assert len(view_project.samples) == 2
            assert view_project != project

    @pytest.mark.parametrize(
        "namespace, name, sample_name, view_name",
        [
            ["namespace1", "amendments1", "pig_0h", "view1"],
        ],
    )
    def test_create_view_with_incorrect_sample(self, namespace, name, sample_name, view_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            with pytest.raises(SampleNotFoundError):
                agent.view.create(
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
        self, namespace, name, sample_name, view_name
    ):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.view.create(
                "view1",
                {
                    "project_namespace": "namespace1",
                    "project_name": "amendments1",
                    "project_tag": "default",
                    "sample_list": ["pig_0h", "pig_1h", "pig_2h"],
                },
                no_fail=True,
            )
            project = agent.project.get(namespace, name, raw=False)
            view_project = agent.view.get(namespace, name, "default", view_name, raw=False)
            assert len(view_project.samples) == 2
            assert view_project != project

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_delete_view(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.view.create(
                "view1",
                {
                    "project_namespace": namespace,
                    "project_name": name,
                    "project_tag": "default",
                    "sample_list": [sample_name, "pig_1h"],
                },
            )
            assert len(agent.view.get(namespace, name, "default", "view1", raw=False).samples) == 2
            agent.view.delete(namespace, name, "default", "view1")
            with pytest.raises(ViewNotFoundError):
                agent.view.get(namespace, name, "default", "view1", raw=False)
            assert len(agent.project.get(namespace, name, raw=False).samples) == 4

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_add_sample_to_view(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.view.create(
                "view1",
                {
                    "project_namespace": namespace,
                    "project_name": name,
                    "project_tag": "default",
                    "sample_list": [sample_name],
                },
            )
            agent.view.add_sample(namespace, name, "default", "view1", "pig_1h")
            assert len(agent.view.get(namespace, name, "default", "view1", raw=False).samples) == 2

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_add_multiple_samples_to_view(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.view.create(
                "view1",
                {
                    "project_namespace": namespace,
                    "project_name": name,
                    "project_tag": "default",
                    "sample_list": [sample_name],
                },
            )
            agent.view.add_sample(namespace, name, "default", "view1", ["pig_1h", "frog_0h"])
            assert len(agent.view.get(namespace, name, "default", "view1", raw=False).samples) == 3

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_remove_sample_from_view(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.view.create(
                "view1",
                {
                    "project_namespace": namespace,
                    "project_name": name,
                    "project_tag": "default",
                    "sample_list": [sample_name, "pig_1h"],
                },
            )
            agent.view.remove_sample(namespace, name, "default", "view1", sample_name)
            assert len(agent.view.get(namespace, name, "default", "view1", raw=False).samples) == 1
            assert len(agent.project.get(namespace, name, raw=False).samples) == 4

            with pytest.raises(SampleNotInViewError):
                agent.view.remove_sample(namespace, name, "default", "view1", sample_name)

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_add_existing_sample_in_view(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.view.create(
                "view1",
                {
                    "project_namespace": namespace,
                    "project_name": name,
                    "project_tag": "default",
                    "sample_list": [sample_name, "pig_1h"],
                },
            )
            with pytest.raises(SampleAlreadyInView):
                agent.view.add_sample(namespace, name, "default", "view1", sample_name)

    @pytest.mark.parametrize(
        "namespace, name, sample_name, view_name",
        [
            ["namespace1", "amendments1", "pig_0h", "view1"],
        ],
    )
    def test_get_snap_view(self, namespace, name, sample_name, view_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            snap_project = agent.view.get_snap_view(
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
    def test_get_view_list_from_project(self, namespace, name, sample_name, view_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            assert len(agent.view.get_views_annotation(namespace, name, "default").views) == 0
            agent.view.create(
                "view1",
                {
                    "project_namespace": namespace,
                    "project_name": name,
                    "project_tag": "default",
                    "sample_list": [sample_name, "pig_1h"],
                },
            )
            assert len(agent.view.get_views_annotation(namespace, name, "default").views) == 1
