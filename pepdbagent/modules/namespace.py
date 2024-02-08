import logging
from typing import List, Union, Tuple
from collections import Counter
from datetime import datetime, timedelta

from sqlalchemy import distinct, func, or_, select, text
from sqlalchemy.sql.selectable import Select
from sqlalchemy.orm import Session

from pepdbagent.const import DEFAULT_LIMIT, DEFAULT_OFFSET, PKG_NAME, DEFAULT_LIMIT_INFO
from pepdbagent.db_utils import Projects, BaseEngine
from pepdbagent.models import (
    Namespace,
    NamespaceList,
    NamespaceInfo,
    ListOfNamespaceInfo,
    NamespaceStats,
)
from pepdbagent.utils import tuple_converter

_LOGGER = logging.getLogger(PKG_NAME)


class PEPDatabaseNamespace:
    """
    Class that represents project Namespaces in Database.

    While using this class, user can retrieve all necessary metadata about PEPs
    """

    def __init__(self, pep_db_engine: BaseEngine):
        """
        :param pep_db_engine: pepdbengine object with sa engine
        """
        self._sa_engine = pep_db_engine.engine
        self._pep_db_engine = pep_db_engine

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
        _LOGGER.info(f"Getting namespaces annotation with provided info: (query: {query})")
        admin_tuple = tuple_converter(admin)
        return NamespaceList(
            count=self._count_namespace(search_str=query, admin_nsp=admin_tuple),
            limit=limit,
            offset=offset,
            results=self._get_namespace(
                search_str=query,
                admin_nsp=admin_tuple,
                limit=limit,
                offset=offset,
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
        statement = (
            select(
                Projects.namespace,
                func.count(Projects.name).label("number_of_projects"),
                func.sum(Projects.number_of_samples).label("number_of_samples"),
            )
            .group_by(Projects.namespace)
            .select_from(Projects)
        )

        statement = self._add_condition(
            statement=statement,
            search_str=search_str,
            admin_list=admin_nsp,
        )

        statement = statement.limit(limit).offset(offset)

        with Session(self._sa_engine) as session:
            query_results = session.execute(statement).all()

        results_list = []
        for res in query_results:
            results_list.append(
                Namespace(
                    namespace=res.namespace,
                    number_of_projects=res.number_of_projects,
                    number_of_samples=res.number_of_samples,
                )
            )
        return results_list

    def _count_namespace(self, search_str: str = None, admin_nsp: tuple = tuple()) -> int:
        """
        Get number of found namespace. [This function is related to _get_namespaces]

        :param search_str: string of symbols, words, keywords to search in the
            namespace name.
        :param admin_nsp: tuple of namespaces where project can be retrieved if they are privet
        :return: number of found namespaces
        """
        statement = select(
            func.count(distinct(Projects.namespace)).label("number_of_namespaces")
        ).select_from(Projects)
        statement = self._add_condition(
            statement=statement,
            search_str=search_str,
            admin_list=admin_nsp,
        )
        with Session(self._sa_engine) as session:
            query_results = session.execute(statement).one()

        return query_results.number_of_namespaces

    @staticmethod
    def _add_condition(
        statement: Select,
        search_str: str = None,
        admin_list: Union[Tuple[str], List[str], str] = None,
    ) -> Select:
        """
        Add where clause to sqlalchemy statement (in namespace search)

        :param statement: sqlalchemy representation of a SELECT statement.
        :param search_str: search string that has to be found namespace
        :param admin_list: list or string of admin rights to namespace
        :return: sqlalchemy representation of a SELECT statement with where clause.
        """
        if search_str:
            sql_search_str = f"%{search_str}%"
            statement = statement.where(
                or_(
                    Projects.namespace.ilike(sql_search_str),
                )
            )
        statement = statement.where(
            or_(Projects.private.is_(False), Projects.namespace.in_(admin_list))
        )
        return statement

    def info(self, limit: int = DEFAULT_LIMIT_INFO) -> ListOfNamespaceInfo:
        """
        Get list of top n namespaces in the database

        :param limit: limit of results (top namespace )
        :return: number_of_namespaces: int
                 limit: int
                 results: { namespace: str
                            number_of_projects: int
                            }
        """
        total_number_of_namespaces = self._count_namespace()

        statement = (
            select(
                func.count(Projects.namespace).label("number_of_projects"),
                Projects.namespace,
            )
            .select_from(Projects)
            .where(Projects.private.is_(False))
            .limit(limit)
            .order_by(text("number_of_projects desc"))
            .group_by(Projects.namespace)
        )

        with Session(self._sa_engine) as session:
            query_results = session.execute(statement).all()

        list_of_results = []
        for result in query_results:
            list_of_results.append(
                NamespaceInfo(
                    namespace=result.namespace,
                    number_of_projects=result.number_of_projects,
                )
            )
        return ListOfNamespaceInfo(
            number_of_namespaces=total_number_of_namespaces,
            limit=limit,
            results=list_of_results,
        )

    def stats(self, namespace: str = None, monthly: bool = False) -> NamespaceStats:
        """
        Get statistics for project in the namespace or for all projects in the database.

        :param namespace: namespace name [Default: None (all projects)]
        :param monthly: if True, get statistics for the last 3 years monthly, else for the last 3 months daily.
        """
        if monthly:
            number_of_month = 3
        else:
            number_of_month = 12 * 3
        today_date = datetime.today().date() + timedelta(days=1)
        three_month_ago = today_date - timedelta(days=number_of_month * 30 + 1)
        statement_last_update = select(Projects.last_update_date).filter(
            Projects.last_update_date.between(three_month_ago, today_date)
        )
        statement_create_date = select(Projects.submission_date).filter(
            Projects.submission_date.between(three_month_ago, today_date)
        )
        if namespace:
            statement_last_update = statement_last_update.where(Projects.namespace == namespace)
            statement_create_date = statement_create_date.where(Projects.namespace == namespace)

        with Session(self._sa_engine) as session:
            update_results = session.execute(statement_last_update).all()
            create_results = session.execute(statement_create_date).all()

        if monthly:
            year_month_str_submission = [
                dt.submission_date.strftime("%Y-%m") for dt in create_results
            ]
            year_month_str_last_update = [
                dt.last_update_date.strftime("%Y-%m") for dt in update_results
            ]
        else:
            year_month_str_submission = [
                dt.submission_date.strftime("%Y-%m-%d") for dt in create_results
            ]
            year_month_str_last_update = [
                dt.last_update_date.strftime("%Y-%m-%d") for dt in update_results
            ]

        counts_submission = dict(Counter(year_month_str_submission))
        counts_last_update = dict(Counter(year_month_str_last_update))

        return NamespaceStats(
            namespace=namespace,
            projects_updated=counts_last_update,
            projects_created=counts_submission,
        )
