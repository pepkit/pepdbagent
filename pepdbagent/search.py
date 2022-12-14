import psycopg2
import logmuse
import coloredlogs
from .models import ProjectSearchModel, NamespaceSearchModel

from .const import *

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

    def namespace_search(
        self,
        search_str: str = None,
        admin_nsp: tuple = ("",),
        offset: int = DEFAULT_OFFSET,
        limit: int = DEFAULT_LIMIT,
    ) -> NamespaceSearchModel:
        """

        :param search_str:
        :param admin_nsp:
        :param offset:
        :param limit:
        :return: Result of a search which consist of:
            {
                total number of results
                search limit
                search offset
                search results
            }
        """
        return NamespaceSearchModel(
            number_of_results=self._count_find_namespaces(
                search_str=search_str, admin_nsp=admin_nsp
            ),
            limit=limit,
            offset=offset,
            results=self._find_namespaces(
                search_str=search_str, admin_nsp=admin_nsp, offset=offset, limit=limit
            ),
        )

    def project_search(
        self,
        namespace: str,
        admin: bool = False,
        search_str: str = "",
        offset: int = DEFAULT_OFFSET,
        limit: int = DEFAULT_LIMIT,
    ) -> ProjectSearchModel:
        """

        :param namespace: Namespace of the projects
        :param admin: True if user is admin of this namespace [Default: False]
        :param search_str: search string
        :param offset: Search offset
        :param limit: Search limit
        :return: Result of a search which consist of:
            {
                namespace
                limit
                offset
                total number of results
                search results
            }
        """
        return ProjectSearchModel(
            namespace=namespace,
            limit=limit,
            offset=offset,
            number_of_results=self._count_find_project(
                namespace=namespace,
                search_str=search_str,
                admin=admin,
            ),
            results=self._find_project(
                namespace=namespace,
                search_str=search_str,
                admin=admin,
                offset=offset,
                limit=limit,
            ),
        )

    def _count_find_project(
        self, namespace: str, search_str: str = "", admin: bool = False
    ):
        """
        Get number of found projects. [This function is related to _find_projects]
        :param namespace: namespace where to search for a project
        :param search_str: search string. will be searched in name and description information
        :param admin: True, if user is admin for this namespace
        :return: number of found project in specified namespace
        """
        if not admin:
            admin_str = f"""and {ANNO_COL}->>'{IS_PRIVATE_KEY}' = 'false'"""
        else:
            admin_str = ""
        count_sql = f"""
        select count(*)
            from {DB_TABLE_NAME} 
                where ({NAME_COL} LIKE '%%{search_str}%%' or ({ANNO_COL}->>'description') like '%%{search_str}%%') 
                    and {NAMESPACE_COL} = '{namespace}' {admin_str};
        """
        result = self.__run_sql_fetchall(count_sql)
        try:
            number_of_prj = result[0][0]
        except KeyError:
            number_of_prj = 0
        return number_of_prj

    def _find_project(
        self,
        namespace: str,
        search_str: str = "",
        admin: bool = False,
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET,
    ):
        if not admin:
            admin_str = f"""and {ANNO_COL}->>'{IS_PRIVATE_KEY}' = 'false'"""
        else:
            admin_str = ""
        count_sql = f"""
        select {NAMESPACE_COL}, {NAME_COL}, {TAG_COL}, ({ANNO_COL}->>'number_of_samples')::int, ({ANNO_COL}->>'description')
            from {DB_TABLE_NAME} 
                where ({NAME_COL} LIKE '%%{search_str}%%' or ({ANNO_COL}->>'description') like '%%{search_str}%%') 
                    and {NAMESPACE_COL} = '{namespace}' {admin_str} 
                        LIMIT {limit} OFFSET {offset};
        """
        results = self.__run_sql_fetchall(count_sql)
        results_list = []
        try:
            for res in results:
                results_list.append(
                    {
                        "namespace": res[0],
                        "name": res[1],
                        "tag": res[2],
                        "number_of_samples": res[3],
                        "description": res[4],
                    }
                )
        except KeyError:
            results_list = []

        return results_list

    def _count_find_namespaces(
        self, search_str: str = "", admin_nsp: tuple = None
    ) -> int:
        """
        Get number of found namespace. [This function is related to _find_namespaces]
        :param search_str: string of symbols, words, keywords to search in the
            namespace name.
        :param admin_nsp: tuple of namespaces where project can be retrieved if they are privet
        :return: number of found namespaces
        """
        count_sql = f"""
        select COUNT(DISTINCT ({NAMESPACE_COL}))
            from {DB_TABLE_NAME} where (({NAMESPACE_COL} LIKE '%%{search_str}%%' and ({ANNO_COL}->>'{IS_PRIVATE_KEY}' = 'false' or {ANNO_COL}->>'{IS_PRIVATE_KEY}'  IS NULL) )
                or ({NAMESPACE_COL} LIKE '%%{search_str}%%' and {NAMESPACE_COL} in %s )) 
        """
        result = self.__run_sql_fetchall(count_sql, admin_nsp)
        try:
            number_of_prj = result[0][0]
        except KeyError:
            number_of_prj = 0
        return number_of_prj

    def _find_namespaces(
        self,
        search_str: str,
        admin_nsp: tuple = None,
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET,
    ) -> list:
        """
        Search for namespace by providing search string
        :param search_str: string of symbols, words, keywords to search in the
            namespace name.
        :param admin_nsp: tuple of namespaces where project can be retrieved if they are privet
        :param limit: limit of return results
        :param offset: number of results off set (that were already showed)
        :return: list of dict with strucutre {
                namespace,
                number_of_projects,
                number_of_samples,
            }
        """
        count_sql = f"""
        select {NAMESPACE_COL}, COUNT({NAME_COL}), SUM( ({ANNO_COL}->>'number_of_samples')::int)
            from {DB_TABLE_NAME} where (({NAMESPACE_COL} LIKE '%%{search_str}%%' and ({ANNO_COL}->>'{IS_PRIVATE_KEY}' = 'false' or {ANNO_COL}->>'{IS_PRIVATE_KEY}'  IS NULL) )
                or ({NAMESPACE_COL} LIKE '%%{search_str}%%' and {NAMESPACE_COL} in %s )) 
                    GROUP BY {NAMESPACE_COL}
                        LIMIT {limit} OFFSET {offset};
        """
        results = self.__run_sql_fetchall(count_sql, admin_nsp)
        results_list = []
        try:
            for res in results:
                results_list.append(
                    {
                        "namespace": res[0],
                        "number_of_projects": res[1],
                        "number_of_samples": res[2],
                    }
                )
        except KeyError:
            results_list = []

        return results_list

    def __run_sql_fetchall(self, sql_query: str, *argv) -> list:
        """
        Fetching all result by providing sql query and arguments
        :param str sql_query: sql string that has to run
        :param argv: arguments that has to be added to sql query
        :return: set of query result
        """
        cursor = self.db_conn.cursor()
        try:
            cursor.execute(sql_query, (*argv,))
            output_result = cursor.fetchall()
            cursor.close()
            return output_result
        except psycopg2.Error as e:
            _LOGGER.error(f"Error occurred while running query: {e}")
        finally:
            cursor.close()
