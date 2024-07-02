import os
import peppy
import warnings
from sqlalchemy.exc import OperationalError

from pepdbagent import PEPDatabaseAgent

DSN = "postgresql+psycopg://postgres:pass8743hf9h23f87h437@localhost:5432/pep-db"

DATA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests",
    "data",
)


def get_path_to_example_file(namespace: str, project_name: str) -> str:
    """
    Get path to example file
    """
    return os.path.join(DATA_PATH, namespace, project_name, "project_config.yaml")


def list_of_available_peps() -> dict:
    pep_namespaces = os.listdir(DATA_PATH)
    projects = {}
    for np in pep_namespaces:
        pep_name = os.listdir(os.path.join(DATA_PATH, np))
        projects[np] = {p: get_path_to_example_file(np, p) for p in pep_name}
    return projects


class PEPDBAgentContextManager:
    """
    Class with context manager to connect to database. Adds data and drops everything from the database upon exit to ensure.
    """

    def __init__(self, url: str = DSN, add_data: bool = False):
        """
        :param url: database url e.g. "postgresql+psycopg://postgres:docker@localhost:5432/pep-db"
        :param add_data: add data to the database
        """

        self.url = url
        self._agent = None
        self.add_data = add_data

    def __enter__(self):
        self._agent = PEPDatabaseAgent(dsn=self.url, echo=False)
        self.db_engine = self._agent.pep_db_engine
        self.db_engine.create_schema()
        if self.add_data:
            self._insert_data()
        return self._agent

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.db_engine.delete_schema()

    def _insert_data(self):
        pepdb_con = PEPDatabaseAgent(dsn=self.url, echo=True)
        for namespace, item in list_of_available_peps().items():
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

    @property
    def agent(self) -> PEPDatabaseAgent:
        return self._agent

    def db_setup(self):
        # Check if the database is setup
        try:
            PEPDatabaseAgent(dsn=self.url)
        except OperationalError:
            warnings.warn(
                UserWarning(
                    f"Skipping tests, because DB is not setup. {self.url}. To setup DB go to README.md"
                )
            )
            return False
        return True
