from pepdbagent.const import POSTGRES_DIALECT
from pepdbagent.db_utils import BaseEngine
from pepdbagent.modules.annotation import PEPDatabaseAnnotation
from pepdbagent.modules.namespace import PEPDatabaseNamespace
from pepdbagent.modules.project import PEPDatabaseProject


class PEPDatabaseAgent(object):
    def __init__(
        self,
        host="localhost",
        port=5432,
        database="pep-db",
        user=None,
        password=None,
        drivername=POSTGRES_DIALECT,
        dsn=None,
        echo=False,
    ):
        """
        Initialize connection to the pep_db database. You can use The basic connection parameters
        or libpq connection string.
        :param host: database server address e.g., localhost or an IP address.
        :param port: the port number that defaults to 5432 if it is not provided.
        :param database: the name of the database that you want to connect.
        :param user: the username used to authenticate.
        :param password: password used to authenticate.
        :param drivername: driver of the database [Default: postgresql]
        :param dsn: libpq connection string using the dsn parameter
        (e.g. "localhost://username:password@pdp_db:5432")
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
        )
        sa_engine = pep_db_engine.engine

        self.__sa_engine = sa_engine

        self.__project = PEPDatabaseProject(pep_db_engine)
        self.__annotation = PEPDatabaseAnnotation(pep_db_engine)
        self.__namespace = PEPDatabaseNamespace(pep_db_engine)

        self.__db_name = database

    @property
    def project(self):
        return self.__project

    @property
    def annotation(self):
        return self.__annotation

    @property
    def namespace(self):
        return self.__namespace

    def __str__(self):
        return f"Connection to the database: '{self.__db_name}' is set!"

    def __exit__(self):
        self.__sa_engine.__exit__()

    @property
    def connection(self):
        return self.__sa_engine
