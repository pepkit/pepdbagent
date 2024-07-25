import pytest

from pepdbagent.exceptions import ProjectAlreadyInFavorites, ProjectNotInFavorites

from .utils import PEPDBAgentContextManager


@pytest.mark.skipif(
    not PEPDBAgentContextManager().db_setup(),
    reason="DB is not setup",
)
class TestNamespace:
    """
    Test function within namespace class
    """

    def test_annotation(self):
        with PEPDBAgentContextManager(add_data=True) as agent:
            result = agent.namespace.get()
            assert len(result.results) == 3

    def test_annotation_private(self):
        with PEPDBAgentContextManager(add_data=True) as agent:
            result = agent.namespace.get(admin="private_test")
            assert len(result.results) == 4

    @pytest.mark.skip(
        "Skipping test because we are not taking into account the private projects (We are counting all of them)"
    )
    def test_namespace_info_private(self):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.project.update(
                namespace="private_test",
                name="derive",
                tag="default",
                update_dict={"is_private": False},
            )
            result = agent.namespace.info()
            assert len(result.results) == 4
            assert result.results[3].number_of_projects == 1

    def test_namespace_info_all(self):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.project.update(
                namespace="private_test",
                name="derive",
                tag="default",
                update_dict={"is_private": False},
            )
            result = agent.namespace.info()
            assert len(result.results) == 4
            assert result.results[3].number_of_projects == 6

    def test_namespace_stats(self):
        with PEPDBAgentContextManager(add_data=True) as agent:
            stat_result = agent.namespace.stats(monthly=True)
            assert next(iter(stat_result.projects_created.values()), 0) == 30


@pytest.mark.skipif(
    not PEPDBAgentContextManager().db_setup(),
    reason="DB is not setup",
)
class TestFavorites:
    """
    Test method related to favorites
    """

    def test_add_projects_to_favorites(self):
        with PEPDBAgentContextManager(add_data=True) as agent:
            result = agent.annotation.get(
                namespace="namespace1",
            )
            for project in result.results:
                agent.user.add_project_to_favorites(
                    "random_namespace", project.namespace, project.name, "default"
                )
            fav_results = agent.user.get_favorites("random_namespace")

            assert fav_results.count == len(result.results)

        # This can fail if the order of the results is different
        assert fav_results.results[0].namespace == result.results[0].namespace

    def test_count_project_none(self):
        with PEPDBAgentContextManager(add_data=True) as agent:
            result = agent.user.get_favorites("private_test")
            assert result.count == 0

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
        ],
    )
    def test_count_project_one(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.user.add_project_to_favorites(namespace, namespace, name, "default")
            result = agent.user.get_favorites("namespace1")
            assert result.count == 1
            result1 = agent.user.get_favorites("private_test")
            assert result1.count == 0

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
        ],
    )
    def test_remove_from_favorite(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.user.add_project_to_favorites("namespace1", namespace, name, "default")
            agent.user.add_project_to_favorites("namespace1", namespace, "amendments2", "default")
            result = agent.user.get_favorites("namespace1")
            assert result.count == len(result.results) == 2
            agent.user.remove_project_from_favorites("namespace1", namespace, name, "default")
            result = agent.user.get_favorites("namespace1")
            assert result.count == len(result.results) == 1

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
        ],
    )
    def test_remove_from_favorite_error(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            with pytest.raises(ProjectNotInFavorites):
                agent.user.remove_project_from_favorites("namespace1", namespace, name, "default")

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
        ],
    )
    def test_favorites_duplication_error(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.user.add_project_to_favorites("namespace1", namespace, name, "default")
            with pytest.raises(ProjectAlreadyInFavorites):
                agent.user.add_project_to_favorites("namespace1", namespace, name, "default")

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "amendments1"],
        ],
    )
    def test_annotation_favorite_number(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.user.add_project_to_favorites("namespace1", namespace, name, "default")
            annotations_in_namespace = agent.annotation.get("namespace1")

            for prj_annot in annotations_in_namespace.results:
                if prj_annot.name == name:
                    assert prj_annot.stars_number == 1
                else:
                    assert prj_annot.stars_number == 0


@pytest.mark.skipif(
    not PEPDBAgentContextManager().db_setup(),
    reason="DB is not setup",
)
class TestUser:
    """
    Test methods within user class
    """

    def test_create_user(self):
        with PEPDBAgentContextManager(add_data=True) as agent:

            agent.user.create_user("test_user")

            assert agent.user.exists("test_user")

    def test_delete_user(self):
        with PEPDBAgentContextManager(add_data=True) as agent:

            test_user = "test_user"
            agent.user.create_user(test_user)
            assert agent.user.exists(test_user)
            agent.user.delete(test_user)
            assert not agent.user.exists(test_user)

    def test_delete_user_deletes_projects(self):
        with PEPDBAgentContextManager(add_data=True) as agent:

            test_user = "namespace1"

            assert agent.user.exists(test_user)
            agent.user.delete(test_user)
            assert not agent.user.exists(test_user)
            results = agent.namespace.get(query=test_user)
            assert len(results.results) == 0
