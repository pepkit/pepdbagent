import os
import pytest
import peppy
from pepagent import PepAgent
from dotenv import load_dotenv

load_dotenv()

EXAMPLE_NAMESPACES = [
    "nfcore",
    "geo",
    "demo"
]

EXAMPLE_REGISTRIES = [
    "geo/GSE102804", 
    "demo/basic", 
    "nfcore/demo_rna_pep"
]

class TestDatafetching():

    db = PepAgent(
            user=os.environ.get('POSTGRES_USER') or "postgres",
            password=os.environ.get('POSTGRES_PASSWORD') or "docker"
        )
    def test_connection(self):
        assert isinstance(self.db, PepAgent)

    @pytest.mark.parametrize('registry', EXAMPLE_REGISTRIES)
    def test_get_project_by_registry(self, registry):
        project = self.db.get_project(registry)
        assert isinstance(project, peppy.Project)

    def test_get_projects_by_list(self): 
        projects = self.db.get_projects(EXAMPLE_REGISTRIES)
        assert len(projects) == 3

    def test_get_projects_by_registry_path(self):
        projects = self.db.get_projects(EXAMPLE_REGISTRIES[0])
        assert len(projects) == 1

    def test_get_projects_by_namespace(self):
        projects = self.db.get_projects(namespace=EXAMPLE_NAMESPACES[0])
        assert len(projects) == 2

    @pytest.mark.parametrize('namespace', EXAMPLE_NAMESPACES)
    def test_get_namespace(self, namespace: str):
        result = self.db.get_namespace(namespace)
        assert isinstance(result, dict)
        assert 'projects' in result
        assert len(result['projects']) > 0