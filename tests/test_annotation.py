import datetime

import pytest

from pepdbagent.exceptions import FilterError, ProjectNotFoundError

from .utils import PEPDBAgentContextManager


@pytest.mark.skipif(
    not PEPDBAgentContextManager().db_setup(),
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
    def test_annotation_of_one_project(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            result = agent.annotation.get(
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
    def test_annotation_of_one_non_existing_project(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            with pytest.raises(ProjectNotFoundError):
                agent.annotation.get(
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
    def test_annotation_all(self, namespace, n_projects):
        with PEPDBAgentContextManager(add_data=True) as agent:
            result = agent.annotation.get(
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
    def test_annotation_all_private(self, namespace, n_projects, admin):
        with PEPDBAgentContextManager(add_data=True) as agent:
            result = agent.annotation.get(namespace=namespace, admin=admin)
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
    def test_annotation_limit(self, namespace, limit, admin, n_projects):
        with PEPDBAgentContextManager(add_data=True) as agent:
            result = agent.annotation.get(namespace=namespace, admin=admin, limit=limit)
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
    def test_order_by(self, namespace, admin, order_by, first_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            result = agent.annotation.get(namespace=namespace, admin=admin, order_by=order_by)
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
    def test_order_by_desc(self, namespace, admin, order_by, last_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            result = agent.annotation.get(
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
    def test_name_search(self, namespace, query, found_number):
        with PEPDBAgentContextManager(add_data=True) as agent:
            result = agent.annotation.get(namespace=namespace, query=query)
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
    def test_name_search_private(self, namespace, query, found_number):
        with PEPDBAgentContextManager(add_data=True) as agent:
            result = agent.annotation.get(namespace=namespace, query=query, admin="private_test")
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
    def test_all_annotations_are_returned(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            result = agent.annotation.get(
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
    def test_search_filter_success(self, namespace, query, found_number):
        with PEPDBAgentContextManager(add_data=True) as agent:
            date_now = datetime.datetime.now() + datetime.timedelta(days=1)
            date_old = datetime.datetime.now() - datetime.timedelta(days=5)
            result = agent.annotation.get(
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
    def test_search_filter_zero_prj(self, namespace, query, found_number):
        with PEPDBAgentContextManager(add_data=True) as agent:
            date_now = datetime.datetime.now() - datetime.timedelta(days=2)
            date_old = date_now - datetime.timedelta(days=2)
            result = agent.annotation.get(
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
    def test_search_incorrect_filter_by_string(self, namespace, query, found_number):
        with PEPDBAgentContextManager(add_data=True) as agent:
            date_now = datetime.datetime.now() - datetime.timedelta(days=2)
            date_old = date_now - datetime.timedelta(days=2)
            with pytest.raises(FilterError):
                agent.annotation.get(
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
    def test_get_annotation_by_rp_list(self, rp_list, admin, found_number):
        with PEPDBAgentContextManager(add_data=True) as agent:

            result = agent.annotation.get_by_rp_list(rp_list)
            assert len(result.results) == found_number

    def test_get_annotation_by_rp_enpty_list(self):
        with PEPDBAgentContextManager(add_data=True) as agent:
            result = agent.annotation.get_by_rp_list([])
            assert len(result.results) == 0

    @pytest.mark.parametrize(
        "namespace, query, found_number",
        [
            ["namespace1", "ame", 2],
        ],
    )
    def test_search_incorrect_incorrect_pep_type(self, namespace, query, found_number):
        with PEPDBAgentContextManager(add_data=True) as agent:

            with pytest.raises(ValueError):
                agent.annotation.get(namespace=namespace, pep_type="incorrect")

    @pytest.mark.parametrize(
        "namespace, query, found_number",
        [
            ["namespace1", "ame", 2],
        ],
    )
    def test_project_list_without_annotation(self, namespace, query, found_number):
        with PEPDBAgentContextManager(add_data=True) as agent:
            result = agent.annotation.get_projects_list(
                namespace=namespace,
                search_str=query,
            )
            assert len(result) == found_number
