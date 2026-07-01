from pepdbagent.const import POSTGRES_DIALECT
from pepdbagent.db_utils import BaseEngine
from pepdbagent.modules.annotation import PEPDatabaseAnnotation
from pepdbagent.modules.namespace import PEPDatabaseNamespace
from pepdbagent.modules.project import PEPDatabaseProject
from pepdbagent.modules.sample import PEPDatabaseSample
from pepdbagent.modules.schema import PEPDatabaseSchema
from pepdbagent.modules.user import PEPDatabaseUser
from pepdbagent.modules.view import PEPDatabaseView


class PEPDatabaseAgent(object):
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "pep-db",
        user: str | None = None,
        password: str | None = None,
        drivername: str = POSTGRES_DIALECT,
        dsn: str | None = None,
        echo: bool = False,
        run_migrations: bool = False,
    ):
        """Initialize connection to the pep_db database.

        Args:
            host: Database server address, e.g., localhost or an IP address.
            port: Port number (default: 5432).
            database: Name of the database to connect to.
            user: Username for authentication.
            password: Password for authentication.
            drivername: Database driver (default: postgresql).
            dsn: libpq connection string, e.g., "localhost://username:password@pdp_db:5432".
            echo: Log all SQL statements if True.
            run_migrations: Run migrations on the database if True.
        """

        pep_db_engine = BaseEngine(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            drivername=drivername,
            dsn=dsn,
            echo=echo,
            run_migrations=run_migrations,
        )
        sa_engine = pep_db_engine.engine

        self.pep_db_engine = pep_db_engine
        self._sa_engine = sa_engine

        self._project = PEPDatabaseProject(pep_db_engine)
        self._annotation = PEPDatabaseAnnotation(pep_db_engine)
        self._namespace = PEPDatabaseNamespace(pep_db_engine)
        self._sample = PEPDatabaseSample(pep_db_engine)
        self._user = PEPDatabaseUser(pep_db_engine)
        self._view = PEPDatabaseView(pep_db_engine)
        self._schema = PEPDatabaseSchema(pep_db_engine)

        self._db_name = database

    @property
    def project(self) -> PEPDatabaseProject:
        return self._project

    @property
    def annotation(self) -> PEPDatabaseAnnotation:
        return self._annotation

    @property
    def namespace(self) -> PEPDatabaseNamespace:
        return self._namespace

    @property
    def user(self) -> PEPDatabaseUser:
        return self._user

    @property
    def sample(self) -> PEPDatabaseSample:
        return self._sample

    @property
    def view(self) -> PEPDatabaseView:
        return self._view

    @property
    def schema(self) -> PEPDatabaseSchema:
        return self._schema

    def __str__(self):
        return f"Connection to the database: '{self.__db_name}' is set!"

    def __exit__(self):
        self._sa_engine.__exit__()

    @property
    def connection(self) -> BaseEngine:
        return self._sa_engine
