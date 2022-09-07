import os
import pytest
import peppy
from pepdbagent import Connection

# from dotenv import load_dotenv

# load_dotenv()

EXAMPLE_NAMESPACES = ["nfcore", "geo", "demo"]

EXAMPLE_REGISTRIES = [
    "geo/GSE102804:default",
    "demo/basic:default",
    "nfcore/demo_rna_pep:default",
]


@pytest.mark.skipif(True, reason="no way of currently testing this")
class TestDatafetching:

    # db = Connection(
    #     user=os.environ.get("POSTGRES_USER") or "postgres",
    #     password=os.environ.get("POSTGRES_PASSWORD") or "docker",
    # )

    def test_connection(self):
        assert isinstance(self.db, Connection)

    @pytest.mark.parametrize("registry", EXAMPLE_REGISTRIES)
    def test_get_project_by_registry(self, registry):
        project = self.db.get_project_by_registry(registry)
        assert isinstance(project, peppy.Project)

    def test_get_projects_by_list(self):
        projects = self.db.get_projects_in_list(EXAMPLE_REGISTRIES)
        assert len(projects) == 3

    def test_get_projects_by_namespace(self):
        projects = self.db.get_projects_in_namespace(namespace=EXAMPLE_NAMESPACES[0])
        assert len(projects) == 2

    def test_get_namespaces(self):
        namespaces = self.db.get_namespaces_info_by_list()
        assert len(namespaces) > 0

    def test_get_namespace_list(self):
        namespaces = self.db.get_namespaces_info_by_list(names_only=True)
        assert all([isinstance(n, str) for n in namespaces])

    @pytest.mark.parametrize("namespace", EXAMPLE_NAMESPACES)
    def test_get_namespace(self, namespace: str):
        result = self.db.get_namespace_info(namespace)
        assert isinstance(result, dict)
        assert "projects" in result
        assert len(result["projects"]) > 0

    def test_nonexistent_project(self):
        this_registry_doesnt_exist = "blueberry/pancakes"
        with pytest.warns():
            proj = self.db.get_project_by_registry(this_registry_doesnt_exist)
            assert proj is None
