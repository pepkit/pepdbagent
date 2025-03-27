import pytest

from .utils import PEPDBAgentContextManager

from pepdbagent.models import UpdateSchemaVersionFields, UpdateSchemaRecordFields

DEFAULT_SCHEMA_VERSION = "1.0.0"


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
            assert agent.schema.schema_exist(namespace=namespace, name=name)
            assert agent.schema.version_exist(
                namespace=namespace, name=name, version=DEFAULT_SCHEMA_VERSION
            )
            schema = agent.schema.get(
                namespace=namespace, name=name, version=DEFAULT_SCHEMA_VERSION
            )
            assert schema

    def test_update_schema(self):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            new_maintainers = "New Maintainer"
            new_lifecycle_stage = "New Stage"
            new_private = True
            new_name = "new_schema_name"

            agent.schema.update_schema_record(
                "namespace1",
                "2.0.0",
                UpdateSchemaRecordFields(
                    maintainers=new_maintainers,
                    lifecycle_stage=new_lifecycle_stage,
                    private=new_private,
                    name=new_name,
                ),
            )
            result = agent.schema.get_schema_info("namespace1", new_name)
            assert result.maintainers == new_maintainers
            assert result.lifecycle_stage == new_lifecycle_stage
            assert result.private == new_private
            assert result.schema_name == new_name

    def test_update_schema_update_date(self):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            version_name = "2.0.0"
            version_schema = {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "integer"},
                },
            }

            first_time = agent.schema.get_schema_info("namespace1", "2.0.0").last_update_date

            agent.schema.add_version(
                "namespace1",
                version_name,
                "pablo1",
                schema_value=version_schema,
                contributors="Teddy",
                release_notes="Initial release",
            )
            result = agent.schema.get_schema_info("namespace1", "2.0.0")
            assert result.last_update_date != first_time

    def test_add_schema_version(self):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            new_schema = {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "integer"},
                },
            }
            new_contributors = "New Maintainer"
            new_release_notes = "New release"
            agent.schema.update_schema_version(
                "namespace1",
                "2.0.0",
                DEFAULT_SCHEMA_VERSION,
                UpdateSchemaVersionFields(
                    schema_value=new_schema,
                    contributors=new_contributors,
                    release_notes=new_release_notes,
                ),
            )
            result = agent.schema.get_version_info("namespace1", "2.0.0", DEFAULT_SCHEMA_VERSION)
            assert result.contributors == new_contributors
            assert result.release_notes == new_release_notes
            assert agent.schema.get("namespace1", "2.0.0", DEFAULT_SCHEMA_VERSION) == new_schema

    def test_search_schema_version(self):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            result = agent.schema.query_schema_version("namespace1", "2.0.0")
            assert result.pagination.total == 1
            assert len(result.results) == 1

    def test_search_schema_version_with_tags(self):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            schema1 = {
                "type": "object",
                "properties": {
                    "name1": {"type": "string"},
                    "age1": {"type": "integer"},
                },
            }
            schema2 = {
                "type": "object",
                "properties": {
                    "name2": {"type": "string"},
                    "age2": {"type": "integer"},
                },
            }

            agent.schema.add_version(
                "namespace1",
                "2.0.0",
                "bino1",
                schema_value=schema1,
                tags=["tag1", "bioinfo"],
                release_notes="computer change",
            )
            agent.schema.add_version(
                "namespace1",
                "2.0.0",
                "bino2",
                schema_value=schema2,
                tags=["bioinfo"],
                release_notes="language",
            )

            result = agent.schema.query_schema_version("namespace1", "2.0.0", tag="tag1")

            assert result.pagination.total == 1
            assert len(result.results) == 1

            result = agent.schema.query_schema_version("namespace1", "2.0.0", tag="bioinfo")

            assert result.pagination.total == 2
            assert len(result.results) == 2

    def test_search_schema(self):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            result = agent.schema.query_schemas(search_str="bed")
            assert result.pagination.total == 3
            assert len(result.results) == 3

    def test_search_schema_namespace(self):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            result = agent.schema.query_schemas("namespace1")
            assert result.pagination.total == 2
            assert "namespace1" in [f.namespace for f in result.results]
            assert "2.0.0" in [f.schema_name for f in result.results]

    def test_search_schema_page_number(self):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            result = agent.schema.query_schemas("namespace2", page_size=2, page=1)
            assert result.pagination.total == 3
            assert result.pagination.page == 1
            assert result.pagination.page_size == 2
            assert len(result.results) == 1

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
            assert not agent.schema.version_exist(
                namespace=namespace, name=name, version=DEFAULT_SCHEMA_VERSION
            )
            assert not agent.schema.schema_exist(namespace=namespace, name=name)

    @pytest.mark.parametrize(
        "namespace, name",
        [
            ["namespace2", "bedmaker"],
        ],
    )
    def test_schema_version_delete(self, namespace, name):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            assert agent.schema.version_exist(
                namespace=namespace, name=name, version=DEFAULT_SCHEMA_VERSION
            )
            agent.schema.delete_version(
                namespace=namespace, name=name, version=DEFAULT_SCHEMA_VERSION
            )
            assert not agent.schema.version_exist(
                namespace=namespace, name=name, version=DEFAULT_SCHEMA_VERSION
            )
            assert agent.schema.schema_exist(namespace=namespace, name=name)

    def test_number_of_schemas_in_namespace(self):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            for k in agent.namespace.info().results:
                if k.namespace_name == "namespace1":
                    assert k.number_of_schemas == 2
                if k.namespace_name == "namespace2":
                    assert k.number_of_schemas == 3


class TestSchemaTags:
    def test_insert_tags(self):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            new_tag1 = "new_tag"
            new_tag2 = "tag2"
            agent.schema.add_tag_to_schema(
                "namespace1", "2.0.0", DEFAULT_SCHEMA_VERSION, tag=[new_tag1, new_tag2]
            )

            result = agent.schema.get_version_info("namespace1", "2.0.0", DEFAULT_SCHEMA_VERSION)

            assert new_tag1 in result.tags
            assert new_tag2 in result.tags

    def test_insert_one_tag(self):
        with PEPDBAgentContextManager(add_schemas=True) as agent:
            new_tag1 = "new_tag"
            agent.schema.add_tag_to_schema(
                "namespace1", "2.0.0", DEFAULT_SCHEMA_VERSION, tag=new_tag1
            )
            result = agent.schema.get_version_info("namespace1", "2.0.0", DEFAULT_SCHEMA_VERSION)
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
            agent.schema.add_tag_to_schema(namespace, name, DEFAULT_SCHEMA_VERSION, tag=new_tag1)
            result = agent.schema.get_version_info(namespace, name, DEFAULT_SCHEMA_VERSION)
            assert new_tag1 in result.tags
            agent.schema.remove_tag_from_schema(
                namespace, name, DEFAULT_SCHEMA_VERSION, tag=new_tag1
            )
            result = agent.schema.get_version_info(namespace, name, DEFAULT_SCHEMA_VERSION)
            assert new_tag1 not in result.tags
