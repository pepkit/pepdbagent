import psycopg2
import logmuse
import coloredlogs

from const import DEFAULT_OFFSET, DEFAULT_LIMIT

_LOGGER = logmuse.init_logger("pepDB_connector")
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] %(message)s",
)


class Search:
    def __init__(self, db_conn: psycopg2):
        """
        Search function using already build connection
        """
        self.db_conn = db_conn

    def namespace_search(self, search_str: str = None,
                         offset: int = DEFAULT_OFFSET,
                         limit: int = DEFAULT_LIMIT):
        return "dupa"

    def project_search(self, namespace: str,
                       search_str: str = None,
                       offset: int = DEFAULT_OFFSET,
                       limit: int = DEFAULT_LIMIT):
        sql_query = """
            select count(*) from projects WHERE name LIKE '%20%' and namespace = 'new' LIMIT 4 OFFSET 0;
        """
        sql_query = """
            select * from projects WHERE name LIKE '%20%' and namespace = 'new' LIMIT 4 OFFSET 0;
        """
        ggg = self.__run_sql_fetchall(sql_query)
        for g in ggg:
            print(g)


    def _count_find_elements_project(self):
        ...

    def _count_find_elements_namespace(self):
        ...



    def __run_sql_fetchall(self, sql_query: str, *argv) -> list:
        """
        Fetching all result by providing sql query and arguments
        :param str sql_query: sql string that has to run
        :param argv: arguments that has to be added to sql query
        :return: set of query result
        """
        cursor = self.db_conn.cursor()
        if not argv:
            argv = None
        try:
            cursor.execute(sql_query, argv)
            output_result = cursor.fetchall()
            cursor.close()
            return output_result
        except psycopg2.Error as e:
            _LOGGER.error(f"Error occurred while running query: {e}")
        finally:
            cursor.close()