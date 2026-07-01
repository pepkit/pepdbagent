import logging
from collections import Counter
from datetime import datetime, timedelta

from sqlalchemy import delete, distinct, func, or_, select
from sqlalchemy.orm import Session
from sqlalchemy.sql.selectable import Select

from pepdbagent.const import DEFAULT_LIMIT, DEFAULT_LIMIT_INFO, DEFAULT_OFFSET, PKG_NAME
from pepdbagent.db_utils import BaseEngine, Projects, TarNamespace, User
from pepdbagent.exceptions import NamespaceNotFoundError
from pepdbagent.models import (
    ListOfNamespaceInfo,
    Namespace,
    NamespaceInfo,
    NamespaceList,
    NamespaceStats,
    PaginationResult,
    TarNamespaceModel,
    TarNamespaceModelReturn,
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
        Args:
            pep_db_engine: PEPDatabaseAgent engine object.
        """
        self._sa_engine = pep_db_engine.engine
        self._pep_db_engine = pep_db_engine

    def get(
        self,
        query: str = "",
        admin: list[str] | str | None = None,
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET,
    ) -> NamespaceList:
        """Search available namespaces in the database.

        Args:
            query: Search string.
            admin: Namespaces where the user has admin rights.
            limit: Maximum number of results to return.
            offset: Number of results to skip.

        Returns:
            NamespaceList with count, limit, offset, and results.
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
    ) -> list[Namespace]:
        """Search for namespaces matching a search string.

        Args:
            search_str: Keywords to search in namespace names.
            admin_nsp: Namespaces accessible even when private.
            limit: Maximum number of results.
            offset: Number of results to skip.

        Returns:
            List of Namespace objects.
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

    def _count_namespace(
        self, search_str: str = None, admin_nsp: tuple = tuple()
    ) -> int:
        """Count namespaces matching a search string.

        Args:
            search_str: Keywords to search in namespace names.
            admin_nsp: Namespaces accessible even when private.

        Returns:
            Number of matching namespaces.
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
        admin_list: tuple[str, ...] | list[str] | str | None = None,
    ) -> Select:
        """Add a WHERE clause to a namespace search statement.

        Args:
            statement: SQLAlchemy SELECT statement to augment.
            search_str: String to search in namespace names.
            admin_list: Namespaces with admin/private access.

        Returns:
            Statement with WHERE clause applied.
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
        """Get a paginated list of top namespaces.

        Warning: counts all projects including private ones (by design, for efficiency).

        Args:
            page: Page number (zero-based).
            page_size: Number of namespaces per page.
            order_by: Sort field — "number_of_projects" or "number_of_schemas".

        Returns:
            ListOfNamespaceInfo with pagination metadata and results.
        """

        statement = select(User)

        if order_by == "number_of_projects":
            statement = statement.order_by(User.number_of_projects.desc())
        elif order_by == "number_of_schemas":
            statement = statement.order_by(User.number_of_schemas.desc())

        with Session(self._sa_engine) as session:
            results = session.scalars(
                statement.limit(page_size).offset(page_size * page)
            )
            total_number_of_namespaces = session.execute(
                select(func.count(User.id))
            ).one()[0]

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
        """Get submission/update statistics for a namespace or the whole database.

        Args:
            namespace: Namespace to filter by (default: all namespaces).
            monthly: Return monthly stats for 3 years if True, daily stats for 3 months if False.

        Returns:
            NamespaceStats with projects_updated and projects_created histograms.
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
            statement_last_update = statement_last_update.where(
                Projects.namespace == namespace
            )
            statement_create_date = statement_create_date.where(
                Projects.namespace == namespace
            )

        with Session(self._sa_engine) as session:
            update_results = session.execute(statement_last_update).all()
            create_results = session.execute(statement_create_date).all()

        if not update_results:
            raise NamespaceNotFoundError(
                f"Namespace {namespace} not found in the database"
            )

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
        """Upload metadata for a namespace tar archive.

        Args:
            tar_info: Tar archive metadata.
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
        """Get metadata for namespace tar archives.

        Args:
            namespace: Namespace of the tar files.

        Returns:
            TarNamespaceModelReturn with count and list of archive metadata.
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
        """Delete tar archive metadata for a namespace.

        Args:
            namespace: Namespace to delete archives for (default: all namespaces).
        """

        with Session(self._sa_engine) as session:
            delete_statement = delete(TarNamespace)
            if namespace:
                delete_statement = delete_statement.where(
                    TarNamespace.namespace == namespace
                )
            session.execute(delete_statement)
            session.commit()
            _LOGGER.info("Geo tar info was deleted successfully!")
