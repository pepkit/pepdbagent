from typing import Union, List
import logging

from pepdbagent.base_connection import BaseConnection
from pepdbagent.const import (
    DEFAULT_LIMIT,
    DEFAULT_OFFSET,
    NAMESPACE_COL,
    NAME_COL,
    N_SAMPLES_COL,
    DB_TABLE_NAME,
    PRIVATE_COL,
)

from pepdbagent.models import Namespace, NamespaceList
from pepdbagent.utils import tuple_converter

_LOGGER = logging.getLogger("pepdbagent")


class PEPDatabaseNamespace:
    """
    Class that represents project Namespaces in Database.

    While using this class, user can retrieve all necessary metadata about PEPs
    """

    def __init__(self, con: BaseConnection):
        """
        :param con: Connection to db represented by BaseConnection class object
        """
        self.con = con

    def get(
        self,
        query: str = "",
        admin: Union[List[str], str] = None,
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET,
    ) -> NamespaceList:
        """
        Search available namespaces in the database
        :param query: search string
        :param admin: list of namespaces where user is admin
        :param offset: offset of the search
        :param limit: limit of the search
        :return: Search result:
            {
                total number of results
                search limit
                search offset
                search results
            }
        """
        _LOGGER.info(
            f"Getting namespaces annotation with provided info: (query: {query})"
        )
        admin_tuple = tuple_converter(admin)
        return NamespaceList(
            count=self._count_namespace(search_str=query, admin_nsp=admin_tuple),
            limit=limit,
            offset=offset,
            results=self._get_namespace(
                search_str=query, admin_nsp=admin_tuple, limit=limit, offset=offset
            ),
        )

    def _get_namespace(
        self,
        search_str: str,
        admin_nsp: tuple = None,
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET,
    ) -> List[Namespace]:
        """
        Search for namespace by providing search string.
        :param search_str: string of symbols, words, keywords to search in the
            namespace name.
        :param admin_nsp: tuple of namespaces where project can be retrieved if they are privet
        :param limit: limit of return results
        :param offset: number of results off set (that were already showed)
        :return: list of dict with structure {
                namespace,
                number_of_projects,
                number_of_samples,
            }
        """
        if search_str:
            search_str = f"%%{search_str}%%"
            search_sql_values = (search_str,)
            search_sql = f"""{NAMESPACE_COL} ILIKE %s and"""
        else:
            search_sql_values = tuple()
            search_sql = ""

        count_sql = f"""
        select {NAMESPACE_COL}, COUNT({NAME_COL}), SUM({N_SAMPLES_COL})
            from {DB_TABLE_NAME} where {search_sql}
                ({PRIVATE_COL} is %s or {NAMESPACE_COL} in %s) 
                    GROUP BY {NAMESPACE_COL}
                        LIMIT %s OFFSET %s;
        """
        results = self.con.run_sql_fetchall(
            count_sql, *search_sql_values, False, admin_nsp, limit, offset
        )
        results_list = []
        for res in results:
            results_list.append(
                Namespace(
                    namespace=res[0],
                    number_of_projects=res[1],
                    number_of_samples=res[2],
                )
            )
        return results_list

    def _count_namespace(self, search_str: str = None, admin_nsp: tuple = None) -> int:
        """
        Get number of found namespace. [This function is related to _get_namespaces]
        :param search_str: string of symbols, words, keywords to search in the
            namespace name.
        :param admin_nsp: tuple of namespaces where project can be retrieved if they are privet
        :return: number of found namespaces
        """
        if search_str:
            search_str = f"%%{search_str}%%"
            search_sql_values = (search_str,)
            search_sql = f"""{NAMESPACE_COL} ILIKE %s and"""
        else:
            search_sql_values = tuple()
            search_sql = ""
        count_sql = f"""
        select COUNT(DISTINCT ({NAMESPACE_COL}))
            from {DB_TABLE_NAME} where {search_sql}
                ({PRIVATE_COL} is %s or {NAMESPACE_COL} in %s) 
        """
        result = self.con.run_sql_fetchall(
            count_sql, *search_sql_values, False, admin_nsp
        )
        try:
            number_of_prj = result[0][0]
        except KeyError:
            number_of_prj = 0
        return number_of_prj
