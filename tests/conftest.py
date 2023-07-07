import peppy
import pytest
import os

from sqlalchemy import create_engine
from sqlalchemy import text

DNS = f"postgresql://postgres:docker@localhost:5432/pep-db"
from pepdbagent import PEPDatabaseAgent


DATA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests",
    "data",
)


def get_path_to_example_file(namespace, project_name):
    return os.path.join(DATA_PATH, namespace, project_name, "project_config.yaml")


@pytest.fixture
def list_of_available_peps():
    pep_namespaces = os.listdir(DATA_PATH)
    projects = {}
    for np in pep_namespaces:
        pep_name = os.listdir(os.path.join(DATA_PATH, np))
        projects[np] = {p: get_path_to_example_file(np, p) for p in pep_name}
    return projects


@pytest.fixture(scope="function")
def initiate_pepdb_con(
    list_of_available_peps,
):
    sa_engine = create_engine(DNS)
    with sa_engine.begin() as conn:
        conn.execute(text("DROP table IF EXISTS projects CASCADE"))
        conn.execute(text("DROP table IF EXISTS samples CASCADE"))
        conn.execute(text("DROP table IF EXISTS subsamples CASCADE"))
    pepdb_con = PEPDatabaseAgent(dsn=DNS, echo=True)
    for namespace, item in list_of_available_peps.items():
        if namespace == "private_test":
            private = True
        else:
            private = False
        for name, path in item.items():
            prj = peppy.Project(path)
            pepdb_con.project.create(
                namespace=namespace,
                name=name,
                tag="default",
                is_private=private,
                project=prj,
                overwrite=True,
                pep_schema="random_schema_name",
            )

    yield pepdb_con


@pytest.fixture(scope="function")
def initiate_empty_pepdb_con(
    list_of_available_peps,
):
    """
    create connection without adding peps to the db
    """
    # sa_engine = create_engine(DNS)
    # with sa_engine.begin() as conn:
    #     conn.execute(text("DROP table IF EXISTS projects CASCADE"))
    #     conn.execute(text("DROP table IF EXISTS samples CASCADE"))
    #     conn.execute(text("DROP table IF EXISTS subsamples CASCADE"))

    pepdb_con = PEPDatabaseAgent(dsn=DNS, echo=False)

    yield pepdb_con
