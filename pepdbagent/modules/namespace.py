import logging
from collections import Counter
from datetime import datetime, timedelta
from typing import List, Tuple, Union

from sqlalchemy import distinct, func, or_, select, delete
from sqlalchemy.orm import Session
from sqlalchemy.sql.selectable import Select

from pepdbagent.const import DEFAULT_LIMIT, DEFAULT_LIMIT_INFO, DEFAULT_OFFSET, PKG_NAME
from pepdbagent.db_utils import BaseEngine, Projects, User, TarNamespace
from pepdbagent.exceptions import NamespaceNotFoundError
from pepdbagent.models import (
    ListOfNamespaceInfo,
    Namespace,
    NamespaceInfo,
    NamespaceList,
    NamespaceStats,
    TarNamespaceModel,
    TarNamespaceModelReturn,
    PaginationResult,
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

    def info(
        self,
        page: int = 0,
        page_size: int = DEFAULT_LIMIT_INFO,
        order_by: str = "number_of_projects",
    ) -> ListOfNamespaceInfo:
        """
        Get list of top n namespaces in the database
        ! Warning: this function counts number of all projects in namespaces.
        ! it does not filter private projects (It was done for efficiency reasons)

        :param page: page number
        :param page_size: number of namespaces to show
        :param order_by: order by field. Options: number_of_projects, number_of_schemas [Default: number_of_projects]

        :return: number_of_namespaces: int
                 limit: int
                 results: { namespace: str
                            number_of_projects: int
                            number_of_schemas: int
                            }
        """

        statement = select(User)

        if order_by == "number_of_projects":
            statement = statement.order_by(User.number_of_projects.desc())
        elif order_by == "number_of_schemas":
            statement = statement.order_by(User.number_of_schemas.desc())

        with Session(self._sa_engine) as session:
            results = session.scalars(statement.limit(page_size).offset(page_size * page))
            total_number_of_namespaces = session.execute(select(func.count(User.id))).one()[0]

            list_of_results = []
            for result in results:
                list_of_results.append(
                    NamespaceInfo(
                        namespace_name=result.namespace,
                        contact_url=f"https://github.com/{result.namespace}",
                        number_of_projects=result.number_of_projects,
                        number_of_schemas=result.number_of_schemas,
                    )
                )
            return ListOfNamespaceInfo(
                pagination=PaginationResult(
                    page=page,
                    page_size=page_size,
                    total=total_number_of_namespaces,
                ),
                results=list_of_results,
            )

    def stats(self, namespace: str = None, monthly: bool = False) -> NamespaceStats:
        """
        Get statistics for project in the namespace or for all projects in the database.

        :param namespace: namespace name [Default: None (all projects)]
        :param monthly: if True, get statistics for the last 3 years monthly, else for the last 3 months daily.
        """
        if monthly:
            number_of_month = 12 * 3
        else:
            number_of_month = 3
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

        if not update_results:
            raise NamespaceNotFoundError(f"Namespace {namespace} not found in the database")

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

    def upload_tar_info(self, tar_info: TarNamespaceModel) -> None:
        """
        Upload metadata of tar GEO files

        tar_info: TarNamespaceModel
        :return: None
        """

        with Session(self._sa_engine) as session:
            new_tar = TarNamespace(
                file_path=tar_info.file_path,
                namespace=tar_info.namespace,
                creation_date=tar_info.creation_date,
                number_of_projects=tar_info.number_of_projects,
                file_size=tar_info.file_size,
            )
            session.add(new_tar)
            session.commit()

            _LOGGER.info("Geo tar info was uploaded successfully!")

    def get_tar_info(self, namespace: str) -> TarNamespaceModelReturn:
        """
        Get metadata of tar GEO files

        :param namespace: namespace of the tar files

        :return: list with geo data
        """

        with Session(self._sa_engine) as session:
            tar_info = session.scalars(
                select(TarNamespace)
                .where(TarNamespace.namespace == namespace)
                .order_by(TarNamespace.creation_date.desc())
            )

            results = []
            for result in tar_info:
                results.append(
                    TarNamespaceModel(
                        identifier=result.id,
                        namespace=result.namespace,
                        file_path=result.file_path,
                        creation_date=result.creation_date,
                        number_of_projects=result.number_of_projects,
                        file_size=result.file_size,
                    )
                )

        return TarNamespaceModelReturn(count=len(results), results=results)

    def delete_tar_info(self, namespace: str = None) -> None:
        """
        Delete all metadata of tar GEO files

        :param namespace: namespace of the tar files

        :return: None
        """

        with Session(self._sa_engine) as session:

            delete_statement = delete(TarNamespace)
            if namespace:
                delete_statement = delete_statement.where(TarNamespace.namespace == namespace)
            session.execute(delete_statement)
            session.commit()
            _LOGGER.info("Geo tar info was deleted successfully!")
