from urllib.parse import urlparse
import logging
import psycopg2
from .const import *
from .exceptions import SchemaError

_LOGGER = logging.getLogger("pepdbagent")


class BaseConnection:
    """
    A class with base methods, that are used in several classes. e.g. fetch_one or fetch_all
    """

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

        _LOGGER.info(f"Initializing connection to {database}...")

        if dsn is not None:
            self.pg_connection = psycopg2.connect(dsn)
            self.db_name = self._extract_db_name(dsn)
        else:
            self.pg_connection = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
            )
            self.db_name = database

        # Ensure data is added to the database immediately after write commands
        self.pg_connection.autocommit = True

        self._check_conn_db()
        _LOGGER.info(f"Connected successfully!")

    def commit_to_database(self) -> None:
        """
        Commit to database
        """
        self.pg_connection.commit()

    def close_connection(self) -> None:
        """
        Close connection with database
        """
        self.pg_connection.close()

    def __exit__(self):
        self.close_connection()

    def __del__(self):
        self.close_connection()

    def run_sql_fetchone(self, sql_query: str, *argv) -> list:
        """
        Fetching one result by providing sql query and arguments
        :param sql_query: sql string that has to run
        :param argv: arguments that has to be added to sql query
        :return: set of query result
        """
        cursor = self.pg_connection.cursor()
        _LOGGER.debug(f"Running fetch_one function with sql: {sql_query}")
        try:
            cursor.execute(sql_query, argv)
            output_result = cursor.fetchone()

            # must run check here since None is not iterable.
            if output_result is not None:
                return list(output_result)
            else:
                return []
        except psycopg2.Error as e:
            _LOGGER.error(f"Error occurred while running query: {e}")
        finally:
            cursor.close()

    def run_sql_fetchall(self, sql_query: str, *argv) -> list:
        """
        Fetching all result by providing sql query and arguments
        :param str sql_query: sql string that has to run
        :param argv: arguments that has to be added to sql query
        :return: set of query result
        """
        _LOGGER.debug(f"Running fetch_all function with sql: {sql_query}")
        cursor = self.pg_connection.cursor()
        try:
            cursor.execute(sql_query, (*argv,))
            output_result = cursor.fetchall()
            cursor.close()
            return output_result
        except psycopg2.Error as e:
            _LOGGER.error(f"Error occurred while running query: {e}")
        finally:
            cursor.close()

    def _check_conn_db(self) -> None:
        """
        Checking if connected database has correct column_names
        """
        a = f"""
            SELECT *
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = N'{DB_TABLE_NAME}'
            """
        result = self.run_sql_fetchall(a)
        cols_name = []
        for col in result:
            cols_name.append(col[3])
        DB_COLUMNS.sort()
        cols_name.sort()
        if DB_COLUMNS != cols_name:
            raise SchemaError

    @staticmethod
    def _extract_db_name(dsn: str) -> str:
        """
        Extract database name from libpq conncection string
        :param dsn: libpq connection string using the dsn parameter
        :return: database name
        """
        return urlparse(dsn).path[1:]
