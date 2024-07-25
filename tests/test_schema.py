import pytest

from .utils import PEPDBAgentContextManager


@pytest.mark.skipif(
    not PEPDBAgentContextManager().db_setup(),
    reason="DB is not setup",
)
class TestSchemas:

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "2.0.0"],
        ],
    )
    def test_get(self, namespace, name):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            schema = agent.schema.get(namespace=namespace, name=name)
            assert agent.schema.exist(namespace=namespace, name=name)
            assert schema

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "2.0.0"],
        ],
    )
    def test_delete(self, namespace, name):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            assert agent.schema.exist(namespace=namespace, name=name)
            agent.schema.delete(namespace=namespace, name=name)
            assert not agent.schema.exist(namespace=namespace, name=name)

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "2.0.0"],
        ],
    )
    def test_update(self, namespace, name):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            schema = agent.schema.get(namespace=namespace, name=name)
            schema["new"] = "hello"
            agent.schema.update(namespace=namespace, name=name, schema=schema)
            assert agent.schema.exist(namespace=namespace, name=name)
            assert schema == agent.schema.get(namespace=namespace, name=name)

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "2.0.0"],
        ],
    )
    def test_get_annotation(self, namespace, name):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            schema_annot = agent.schema.info(namespace=namespace, name=name)
            assert schema_annot
            assert schema_annot.model_fields_set == {
                "namespace",
                "name",
                "last_update_date",
                "submission_date",
                "description",
                "popularity_number",
            }

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "2.0.0"],
        ],
    )
    def test_update_annotation(self, namespace, name):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            schema_annot = agent.schema.info(namespace=namespace, name=name)
            schema = agent.schema.get(namespace=namespace, name=name)
            agent.schema.update(
                namespace=namespace, name=name, schema=schema, description="new desc"
            )
            assert schema_annot != agent.schema.info(namespace=namespace, name=name)

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace2", "bedboss"],
        ],
    )
    def test_annotation_popular(self, namespace, name):
        with PEPDBAgentContextManager(add_data=True, add_schemas=True) as agent:
            agent.project.update(
                namespace="namespace1",
                name="amendments1",
                update_dict={"pep_schema": "namespace2/bedboss"},
            )
            schema_annot = agent.schema.info(namespace=namespace, name=name)
            assert schema_annot.popularity_number == 1

    def test_search(self):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            results = agent.schema.search(namespace="namespace2")
            assert results
            assert results.count == 3
            assert len(results.results) == 3

    def test_search_offset(self):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            results = agent.schema.search(namespace="namespace2", offset=1)
            assert results
            assert results.count == 3
            assert len(results.results) == 2

    def test_search_limit(self):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            results = agent.schema.search(namespace="namespace2", limit=1)
            assert results
            assert results.count == 3
            assert len(results.results) == 1

    def test_search_limit_offset(self):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            results = agent.schema.search(namespace="namespace2", limit=2, offset=2)
            assert results
            assert results.count == 3
            assert len(results.results) == 1

    def test_search_query(self):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            results = agent.schema.search(namespace="namespace2", search_str="bedb")
            assert results
            assert results.count == 2
            assert len(results.results) == 2

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "2.0.0"],
        ],
    )
    def test_create_group(self, namespace, name):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            group_name = "new_group"
            agent.schema.group_create(
                namespace=namespace, name=group_name, description="new group"
            )
            assert agent.schema.group_exist(namespace=namespace, name=group_name)

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "2.0.0"],
        ],
    )
    def test_delete_group(self, namespace, name):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            group_name = "new_group"
            agent.schema.group_create(
                namespace=namespace, name=group_name, description="new group"
            )
            assert agent.schema.group_exist(namespace=namespace, name=group_name)
            agent.schema.group_delete(namespace=namespace, name=group_name)
            assert not agent.schema.group_exist(namespace=namespace, name=group_name)

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "2.0.0"],
        ],
    )
    def test_add_to_group(self, namespace, name):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            group_name = "new_group"
            agent.schema.group_create(
                namespace=namespace, name=group_name, description="new group"
            )
            agent.schema.group_add_schema(
                namespace=namespace, name=group_name, schema_name=name, schema_namespace=namespace
            )
            group_annot = agent.schema.group_get(namespace=namespace, name=group_name)
            assert group_annot.schemas[0].name == name

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "2.0.0"],
        ],
    )
    def test_remove_from_group(self, namespace, name):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            group_name = "new_group"
            agent.schema.group_create(
                namespace=namespace, name=group_name, description="new group"
            )
            agent.schema.group_add_schema(
                namespace=namespace, name=group_name, schema_name=name, schema_namespace=namespace
            )
            group_annot = agent.schema.group_get(namespace=namespace, name=group_name)
            assert len(group_annot.schemas) == 1

            agent.schema.group_remove_schema(
                namespace=namespace, name=group_name, schema_name=name, schema_namespace=namespace
            )
            group_annot = agent.schema.group_get(namespace=namespace, name=group_name)
            assert len(group_annot.schemas) == 0

    def test_search_group(self):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            group_name1 = "new_group1"
            group_name2 = "new2"
            group_name3 = "new_group3"
            agent.schema.group_create(
                namespace="namespace1", name=group_name1, description="new group"
            )
            agent.schema.group_create(namespace="namespace1", name=group_name2, description="new")
            agent.schema.group_create(
                namespace="namespace1", name=group_name3, description="new group"
            )

            results = agent.schema.group_search(search_str="new_group")

            assert results.count == 2
            assert len(results.results) == 2
