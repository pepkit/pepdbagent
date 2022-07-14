import pytest
import peppy
from pepagent import PepAgent

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

def test_connection(database_credentials: dict):
    db = PepAgent(
        user=database_credentials['POSTGRES_USER'],
        password=database_credentials['POSTGRES_PASSWORD']
    )
    assert isinstance(db, PepAgent)

@pytest.mark.parametrize('registry', EXAMPLE_REGISTRIES)
def test_get_project_by_registry(database_credentials: dict, registry):
    db = PepAgent(
        user=database_credentials['POSTGRES_USER'],
        password=database_credentials['POSTGRES_PASSWORD']
    )
    project = db.get_project(registry)
    assert isinstance(project, peppy.Project)

def test_get_projects_by_list(database_credentials: dict):
    db = PepAgent(
        user=database_credentials['POSTGRES_USER'],
        password=database_credentials['POSTGRES_PASSWORD']
    )
    projects = db.get_projects(EXAMPLE_REGISTRIES)
    assert len(projects) == 3

def test_get_projects_by_registry_path(database_credentials: dict):
    db = PepAgent(
        user=database_credentials['POSTGRES_USER'],
        password=database_credentials['POSTGRES_PASSWORD']
    )
    projects = db.get_projects(EXAMPLE_REGISTRIES[0])
    assert len(projects) == 1

def test_get_projects_by_namespace(database_credentials: dict):
    db = PepAgent(
        user=database_credentials['POSTGRES_USER'],
        password=database_credentials['POSTGRES_PASSWORD']
    )
    projects = db.get_projects(namespace=EXAMPLE_NAMESPACES[0])
    assert len(projects) == 2