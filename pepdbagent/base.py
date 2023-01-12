import logging
import psycopg2

_LOGGER = logging.getLogger("pepdbagent")


class BaseConnection:
    """
    A class with base methods, that are used in several classes. e.g. fetch_one or fetch_all
    """
    def __init__(self, db_conn: psycopg2):
        """
        Coping
        :param db_conn: already existing connection to the db
        """
        self.pg_connection = db_conn

    def _commit_to_database(self) -> None:
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

    def _run_sql_fetchone(self, sql_query: str, *argv) -> list:
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

    def _run_sql_fetchall(self, sql_query: str, *argv) -> list:
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

