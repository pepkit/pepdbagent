import pytest

from .utils import PEPDBAgentContextManager


@pytest.mark.skipif(
    not PEPDBAgentContextManager().db_setup(),
    reason="DB is not setup",
)
class TestSamples:

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace1", "2.0.0"],
        ],
    )
    def test_get(self, namespace, name):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            assert agent.schema.schema_exist(namespace=namespace, name=name)
            assert agent.schema.version_exist(namespace=namespace, name=name, version="1.0.0")
            schema = agent.schema.get(namespace=namespace, name=name, version="1.0.0")
            assert schema

    def test_search_schema(self): ...

    def test_search_schema_namespace(self): ...

    def test_update_schema(self): ...

    def test_update_schema_update_date(self): ...

    def test_add_schema_version(self): ...

    def test_update_schema_version(self): ...

    def test_search_schema_version(self): ...

    def test_search_schema_version_with_tags(self): ...

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace2", "bedmaker"],
        ],
    )
    def test_schema_delete(self, namespace, name):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            assert agent.schema.schema_exist(namespace=namespace, name=name)
            agent.schema.delete_schema(namespace=namespace, name=name)
            assert not agent.schema.version_exist(namespace=namespace, name=name, version="1.0.0")
            assert not agent.schema.schema_exist(namespace=namespace, name=name)

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace2", "bedmaker"],
        ],
    )
    def test_schema_version_delete(self, namespace, name):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            assert agent.schema.version_exist(namespace=namespace, name=name, version="1.0.0")
            agent.schema.delete_version(namespace=namespace, name=name, version="1.0.0")
            assert not agent.schema.version_exist(namespace=namespace, name=name, version="1.0.0")
            assert agent.schema.schema_exist(namespace=namespace, name=name)


class TestSchemaTags:
    def test_insert_tags(self):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            new_tag1 = "new_tag"
            new_tag2 = "tag2"
            agent.schema.add_tag_to_schema(
                "namespace1", "2.0.0", "1.0.0", tag=[new_tag1, new_tag2]
            )

            result = agent.schema.get_version_info("namespace1", "2.0.0", "1.0.0")

            assert new_tag1 in result.tags
            assert new_tag2 in result.tags

    def test_insert_one_tag(self):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            new_tag1 = "new_tag"
            agent.schema.add_tag_to_schema("namespace1", "2.0.0", "1.0.0", tag=new_tag1)
            result = agent.schema.get_version_info("namespace1", "2.0.0", "1.0.0")
            assert new_tag1 in result.tags

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace2", "bedmaker"],
        ],
    )
    def test_delete_tag(self, namespace, name):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            new_tag1 = "new_tag"
            agent.schema.add_tag_to_schema(namespace, name, "1.0.0", tag=new_tag1)
            result = agent.schema.get_version_info(namespace, name, "1.0.0")
            assert new_tag1 in result.tags
            agent.schema.remove_tag_from_schema(namespace, name, "1.0.0", tag=new_tag1)
            result = agent.schema.get_version_info(namespace, name, "1.0.0")
            assert not new_tag1 in result.tags
