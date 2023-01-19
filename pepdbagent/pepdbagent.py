from pepdbagent.base_connection import BaseConnection
from pepdbagent.modules.annotation import PEPDatabaseAnnotation
from pepdbagent.modules.project import PEPDatabaseProject
from pepdbagent.modules.namespace import PEPDatabaseNamespace


class PEPDatabaseAgent(object):
    def __init__(
        self,
        host="localhost",
        port=5432,
        database="pep-db",
        user=None,
        password=None,
        dsn=None,
    ):
        """
        Initialize connection to the pep_db database. You can use The basic connection parameters
        or libpq connection string.
        :param host: database server address e.g., localhost or an IP address.
        :param port: the port number that defaults to 5432 if it is not provided.
        :param database: the name of the database that you want to connect.
        :param user: the username used to authenticate.
        :param password: password used to authenticate.
        :param dsn: libpq connection string using the dsn parameter
        (e.g. "localhost://username:password@pdp_db:5432")
        """

        con = BaseConnection(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            dsn=dsn,
        )
        self.__con = con

        self.__project = PEPDatabaseProject(con)
        self.__annotation = PEPDatabaseAnnotation(con)
        self.__namespace = PEPDatabaseNamespace(con)

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

    def __del__(self):
        self.__con.__del__()

    def __exit__(self):
        self.__con.__exit__()

    @property
    def connection(self):
        return self.__con
