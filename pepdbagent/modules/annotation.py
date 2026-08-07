import logging
from datetime import datetime
from typing import Literal

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session, joinedload, load_only, noload
from sqlalchemy.sql.selectable import Select

from pepdbagent.const import (
    DEFAULT_LIMIT,
    DEFAULT_OFFSET,
    DEFAULT_TAG,
    LAST_UPDATE_DATE_KEY,
    PKG_NAME,
    SUBMISSION_DATE_KEY,
)
from pepdbagent.db_utils import BaseEngine, Projects, SchemaRecords, SchemaVersions
from pepdbagent.exceptions import FilterError, ProjectNotFoundError, RegistryPathError
from pepdbagent.models import AnnotationList, AnnotationModel, RegistryPath
from pepdbagent.utils import (
    convert_date_string_to_date,
    registry_path_converter,
    tuple_converter,
)

_LOGGER = logging.getLogger(PKG_NAME)


class PEPDatabaseAnnotation:
    """
    Class that represents project Annotations in the Database.

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
        namespace: str = None,
        name: str = None,
        tag: str = None,
        query: str = None,
        admin: list[str] | str | None = None,
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET,
        order_by: str = "update_date",
        order_desc: bool = False,
        filter_by: Literal["submission_date", "last_update_date"] | None = None,
        filter_start_date: str | None = None,
        filter_end_date: str | None = None,
        pep_type: Literal["pep", "pop"] | None = None,
    ) -> AnnotationList:
        """Get project annotations.

        Five retrieval scenarios:
        - namespace + name + tag: exact match.
        - namespace only: all projects in that namespace.
        - nothing: all projects in the database.
        - query: full-text search across name, tag, description.
        - query + namespace: full-text search within a namespace.

        Args:
            namespace: Namespace to filter by.
            name: Project name (use with namespace and tag for exact lookup).
            tag: Project tag.
            query: Search string matched against name, tag, and description.
            admin: Namespace(s) where the caller has admin rights.
            limit: Maximum number of results.
            offset: Number of results to skip.
            order_by: Sort field — "name", "update_date", or "submission_date".
            order_desc: Sort in descending order if True.
            filter_by: Date field to apply range filter on — "submission_date" or "last_update_date".
            filter_start_date: Range start in YYYY/MM/DD format.
            filter_end_date: Range end in YYYY/MM/DD format (default: today).
            pep_type: Restrict to "pep" or "pop" (default: all).

        Returns:
            AnnotationList with count, limit, offset, and results.
        """
        if all([namespace, name, tag]):
            found_annotation = [
                self._get_single_annotation(
                    namespace=namespace,
                    name=name,
                    tag=tag,
                    admin=admin,
                )
            ]
            return AnnotationList(
                count=len(found_annotation),
                limit=1,
                offset=0,
                results=found_annotation,
            )

        if pep_type not in [None, "pep", "pop"]:
            raise ValueError(
                f"pep_type should be one of ['pep', 'pop'], got {pep_type}"
            )

        return AnnotationList(
            limit=limit,
            offset=offset,
            count=self._count_projects(
                namespace=namespace,
                search_str=query,
                tag=tag,
                admin=admin,
                filter_by=filter_by,
                filter_end_date=filter_end_date,
                filter_start_date=filter_start_date,
                pep_type=pep_type,
            ),
            results=self._get_projects(
                namespace=namespace,
                search_str=query,
                tag=tag,
                admin=admin,
                offset=offset,
                limit=limit,
                order_by=order_by,
                order_desc=order_desc,
                filter_by=filter_by,
                filter_end_date=filter_end_date,
                filter_start_date=filter_start_date,
                pep_type=pep_type,
            ),
        )

    def get_by_rp(
        self,
        registry_paths: list[str] | str,
        admin: list[str] | str | None = None,
    ) -> AnnotationList:
        """Get project annotations by registry path or list of registry paths.

        Args:
            registry_paths: Single registry path or list of paths ("namespace/name:tag").
            admin: Namespace(s) where the caller has admin rights.

        Returns:
            AnnotationList with count, limit, offset, and results.
        """
        if isinstance(registry_paths, list):
            anno_results = []
            for path in registry_paths:
                try:
                    namespace, name, tag = registry_path_converter(path)
                except RegistryPathError as err:
                    _LOGGER.error(str(err), registry_paths)
                    continue
                try:
                    single_return = self._get_single_annotation(
                        namespace, name, tag, admin
                    )
                    if single_return:
                        anno_results.append(single_return)
                except ProjectNotFoundError:
                    pass
            return_len = len(anno_results)
            return AnnotationList(
                count=return_len,
                limit=len(registry_paths),
                offset=0,
                results=anno_results,
            )

        else:
            namespace, name, tag = registry_path_converter(registry_paths)
            return self.get(namespace=namespace, name=name, tag=tag, admin=admin)

    def _get_single_annotation(
        self,
        namespace: str,
        name: str,
        tag: str = DEFAULT_TAG,
        admin: list[str] | str | None = None,
    ) -> AnnotationModel | None:
        """Retrieve annotation for a single project.

        Args:
            namespace: Project namespace.
            name: Project name.
            tag: Project tag.
            admin: Namespace(s) with admin/private access.

        Returns:
            AnnotationModel for the project, or None if not found.

        Raises:
            ProjectNotFoundError: If the project does not exist or is not accessible.
        """
        _LOGGER.info(f"Getting annotation of the project: '{namespace}/{name}:{tag}'")
        admin_tuple = tuple_converter(admin)

        statement = select(Projects).where(
            and_(
                Projects.name == name,
                Projects.namespace == namespace,
                Projects.tag == tag,
                or_(
                    Projects.namespace.in_(admin_tuple),
                    Projects.private.is_(False),
                ),
            )
        )
        with Session(self._sa_engine) as session:
            query_result = session.scalar(statement)

            if query_result:
                annot = AnnotationModel(
                    namespace=query_result.namespace,
                    name=query_result.name,
                    tag=query_result.tag,
                    is_private=query_result.private,
                    description=query_result.description,
                    number_of_samples=query_result.number_of_samples,
                    submission_date=str(query_result.submission_date),
                    last_update_date=str(query_result.last_update_date),
                    digest=query_result.digest,
                    pep_schema=(
                        f"{query_result.schema_mapping.schema_mapping.namespace}/{query_result.schema_mapping.schema_mapping.name}:{query_result.schema_mapping.version}"
                        if query_result.schema_mapping
                        else None
                    ),
                    pop=query_result.pop,
                    stars_number=query_result.number_of_stars,
                    forked_from=(
                        f"{query_result.forked_from_mapping.namespace}/{query_result.forked_from_mapping.name}:{query_result.forked_from_mapping.tag}"
                        if query_result.forked_from_id
                        else None
                    ),
                )
                _LOGGER.info(
                    f"Annotation of the project '{namespace}/{name}:{tag}' has been found!"
                )
                return annot
            else:
                raise ProjectNotFoundError(
                    f"Project '{namespace}/{name}:{tag}' was not found."
                )

    def _count_projects(
        self,
        namespace: str = None,
        search_str: str = None,
        tag: str = None,
        admin: str | list[str] | None = None,
        filter_by: Literal["submission_date", "last_update_date"] | None = None,
        filter_start_date: str | None = None,
        filter_end_date: str | None = None,
        pep_type: Literal["pep", "pop"] | None = None,
    ) -> int:
        """Count projects matching the given filters.

        Args:
            namespace: Namespace to restrict the search to.
            search_str: String to search in name, tag, and description.
            tag: Exact tag to match.
            admin: Namespace(s) with admin/private access.
            filter_by: Date field for range filter — "submission_date" or "last_update_date".
            filter_start_date: Range start in YYYY/MM/DD format.
            filter_end_date: Range end in YYYY/MM/DD format (default: today).
            pep_type: Restrict to "pep" or "pop" (default: all).

        Returns:
            Number of matching projects.
        """
        if admin is None:
            admin = []
        statement = select(func.count()).select_from(Projects)
        statement = self._add_condition(
            statement,
            namespace=namespace,
            search_str=search_str,
            admin_list=admin,
        )
        statement = self._add_date_filter_if_provided(
            statement, filter_by, filter_start_date, filter_end_date
        )
        if pep_type:
            statement = statement.where(Projects.pop.is_(pep_type == "pop"))
        if tag:
            statement = statement.where(Projects.tag == tag)
        result = self._pep_db_engine.session_execute(statement).first()

        try:
            return result[0]
        except IndexError:
            return 0

    def _get_projects(
        self,
        namespace: str = None,
        tag: str = None,
        search_str: str = None,
        admin: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET,
        order_by: str = "update_date",
        order_desc: bool = False,
        filter_by: Literal["submission_date", "last_update_date"] | None = None,
        filter_start_date: str | None = None,
        filter_end_date: str | None = None,
        pep_type: Literal["pep", "pop"] | None = None,
    ) -> list[AnnotationModel]:
        """Get projects matching the given filters.

        Args:
            namespace: Namespace to restrict the search to.
            tag: Exact tag to match.
            search_str: String to search in name, tag, and description.
            admin: Namespace(s) with admin/private access.
            limit: Maximum number of results.
            offset: Number of results to skip.
            order_by: Sort field — "update_date", "name", "submission_date", or "stars".
            order_desc: Sort in descending order if True.
            filter_by: Date field for range filter — "submission_date" or "last_update_date".
            filter_start_date: Range start in YYYY/MM/DD format.
            filter_end_date: Range end in YYYY/MM/DD format (default: today).
            pep_type: Restrict to "pep" or "pop" (default: all).

        Returns:
            List of AnnotationModel objects.
        """
        _LOGGER.info(
            f"Running annotation search: (namespace: {namespace}, query: {search_str}."
        )

        if admin is None:
            admin = []
        statement = select(Projects).options(*self._annotation_load_options())

        statement = self._add_condition(
            statement,
            namespace=namespace,
            search_str=search_str,
            admin_list=admin,
            tag=tag,
        )
        statement = self._add_date_filter_if_provided(
            statement, filter_by, filter_start_date, filter_end_date
        )
        statement = self._add_order_by_keyword(statement, by=order_by, desc=order_desc)
        statement = statement.limit(limit).offset(offset)
        if pep_type:
            statement = statement.where(Projects.pop.is_(pep_type == "pop"))

        results_list = []
        with Session(self._sa_engine) as session:
            # Unique should be called because of the join with schema_mapping
            results = session.scalars(statement).unique()
            for result in results:
                results_list.append(
                    AnnotationModel(
                        namespace=result.namespace,
                        name=result.name,
                        tag=result.tag,
                        is_private=result.private,
                        description=result.description,
                        number_of_samples=result.number_of_samples,
                        submission_date=str(result.submission_date),
                        last_update_date=str(result.last_update_date),
                        digest=result.digest,
                        pep_schema=(
                            f"{result.schema_mapping.schema_mapping.namespace}/{result.schema_mapping.schema_mapping.name}:{result.schema_mapping.version}"
                            if result.schema_mapping
                            else None
                        ),
                        pop=result.pop,
                        stars_number=result.number_of_stars,
                        forked_from=(
                            f"{result.forked_from_mapping.namespace}/{result.forked_from_mapping.name}:{result.forked_from_mapping.tag}"
                            if result.forked_from_id
                            else None
                        ),
                    )
                )
        return results_list

    @staticmethod
    def _annotation_load_options() -> tuple:
        """Loader options for queries that build AnnotationModel objects.

        Fetch only the needed columns (skipping the config and schema_value
        JSON payloads) and eager-load the schema and fork relationships so no
        per-row lazy queries are issued.
        """
        return (
            load_only(
                Projects.namespace,
                Projects.name,
                Projects.tag,
                Projects.private,
                Projects.description,
                Projects.number_of_samples,
                Projects.submission_date,
                Projects.last_update_date,
                Projects.digest,
                Projects.pop,
                Projects.number_of_stars,
                Projects.forked_from_id,
            ),
            joinedload(Projects.schema_mapping).options(
                load_only(SchemaVersions.version),
                joinedload(SchemaVersions.schema_mapping).load_only(
                    SchemaRecords.namespace, SchemaRecords.name
                ),
                noload(SchemaVersions.tags_mapping),
            ),
            joinedload(Projects.forked_from_mapping).options(
                load_only(Projects.namespace, Projects.name, Projects.tag),
                noload(Projects.schema_mapping),
            ),
        )

    @staticmethod
    def _add_order_by_keyword(
        statement: Select, by: str = "update_date", desc: bool = False
    ) -> Select:
        """Add an ORDER BY clause to a SELECT statement.

        Args:
            statement: SQLAlchemy SELECT statement to augment.
            by: Sort field — "name", "update_date", "submission_date", or "stars".
            desc: Sort in descending order if True.

        Returns:
            Statement with ORDER BY applied.
        """
        if by == "update_date":
            order_by_obj = Projects.last_update_date
        elif by == "name":
            order_by_obj = Projects.name
        elif by == SUBMISSION_DATE_KEY:
            order_by_obj = Projects.submission_date
        elif by == "stars":
            order_by_obj = Projects.number_of_stars
        else:
            _LOGGER.warning(
                f"order by: '{by}' statement is unavailable. Projects are sorted by 'update_date'"
            )
            order_by_obj = Projects.last_update_date

        if desc and by == "name":
            order_by_obj = order_by_obj.desc()

        elif by != "name" and not desc:
            order_by_obj = order_by_obj.desc()

        return statement.order_by(order_by_obj)

    @staticmethod
    def _add_condition(
        statement: Select,
        namespace: str = None,
        search_str: str = None,
        admin_list: str | list[str] | None = None,
        tag: str = None,
    ) -> Select:
        """Add a WHERE clause to a project search statement.

        Args:
            statement: SQLAlchemy SELECT statement to augment.
            namespace: Filter to this namespace.
            search_str: String to search in name, tag, and description.
            admin_list: Namespace(s) with admin/private access.
            tag: Exact tag to match.

        Returns:
            Statement with WHERE clause applied.
        """
        admin_list = tuple_converter(admin_list)
        if search_str:
            sql_search_str = f"%{search_str}%"
            search_query = or_(
                Projects.name.ilike(sql_search_str),
                Projects.tag.ilike(sql_search_str),
                Projects.description.ilike(sql_search_str),
            )
            statement = statement.where(search_query)
        if namespace:
            statement = statement.where(Projects.namespace == namespace)

        if tag:
            statement = statement.where(Projects.tag == tag)

        statement = statement.where(
            or_(Projects.private.is_(False), Projects.namespace.in_(admin_list))
        )

        return statement

    @staticmethod
    def _add_date_filter_if_provided(
        statement: Select,
        filter_by: Literal["submission_date", "last_update_date"] | None,
        filter_start_date: str | None,
        filter_end_date: str | None = None,
    ) -> Select:
        """Add a date range filter to a SELECT statement.

        Args:
            statement: SQLAlchemy SELECT statement to augment.
            filter_by: Date field to filter on — "submission_date" or "last_update_date".
            filter_start_date: Range start in YYYY/MM/DD format.
            filter_end_date: Range end in YYYY/MM/DD format (default: today).

        Returns:
            Statement with date filter applied.
        """
        if filter_by and filter_start_date:
            start_date = convert_date_string_to_date(filter_start_date)
            if filter_end_date:
                end_date = convert_date_string_to_date(filter_end_date)
            else:
                end_date = datetime.now()
            if filter_by == SUBMISSION_DATE_KEY:
                statement = statement.filter(
                    Projects.submission_date.between(start_date, end_date)
                )
            elif filter_by == LAST_UPDATE_DATE_KEY:
                statement = statement.filter(
                    Projects.last_update_date.between(start_date, end_date)
                )
            else:
                raise FilterError("Invalid filter_by was provided!")
            return statement
        else:
            if filter_by:
                _LOGGER.warning(
                    "filter_start_date was not provided, skipping filter..."
                )
            return statement

    def get_project_number_in_namespace(
        self,
        namespace: str,
        admin: str | list[str] | None = None,
    ) -> int:
        """Get the number of projects in a namespace.

        Args:
            namespace: Namespace to count projects in.
            admin: Namespace(s) with admin/private access.

        Returns:
            Number of accessible projects in the namespace.
        """
        if admin is None:
            admin = []
        statement = (
            select(func.count())
            .select_from(Projects)
            .where(Projects.namespace == namespace)
        )
        statement = statement.where(
            or_(Projects.private.is_(False), Projects.namespace.in_(admin))
        )

        result = self._pep_db_engine.session_execute(statement).first()

        try:
            return result[0]
        except IndexError:
            return 0

    def get_by_rp_list(
        self,
        registry_paths: list[str],
        admin: str | list[str] | None = None,
    ) -> AnnotationList:
        """Get project annotations for a list of registry paths in a single query.

        Args:
            registry_paths: List of registry paths ("namespace/name:tag").
            admin: Namespace(s) where the caller has admin rights.

        Returns:
            AnnotationList preserving the input order (missing projects returned as None).
        """
        admin_tuple = tuple_converter(admin)

        if isinstance(registry_paths, list):
            or_statement_list = []
            for path in registry_paths:
                try:
                    namespace, name, tag = registry_path_converter(path)
                    or_statement_list.append(
                        and_(
                            Projects.name == name,
                            Projects.namespace == namespace,
                            Projects.tag == tag,
                            or_(
                                Projects.namespace.in_(admin_tuple),
                                Projects.private.is_(False),
                            ),
                        )
                    )
                except RegistryPathError as err:
                    _LOGGER.error(str(err), registry_paths)
                    continue
            if not or_statement_list:
                _LOGGER.error("No valid registry paths were provided!")
                return AnnotationList(
                    count=0,
                    limit=len(registry_paths),
                    offset=0,
                    results=[],
                )

            statement = (
                select(Projects)
                .options(*self._annotation_load_options())
                .where(or_(*or_statement_list))
            )
            anno_results = []
            with Session(self._sa_engine) as session:
                query_result = session.scalars(statement).unique()
                for result in query_result:
                    project_obj = result
                    annot = AnnotationModel(
                        namespace=project_obj.namespace,
                        name=project_obj.name,
                        tag=project_obj.tag,
                        is_private=project_obj.private,
                        description=project_obj.description,
                        number_of_samples=project_obj.number_of_samples,
                        submission_date=str(project_obj.submission_date),
                        last_update_date=str(project_obj.last_update_date),
                        digest=project_obj.digest,
                        pep_schema=(
                            f"{project_obj.schema_mapping.schema_mapping.namespace}/{project_obj.schema_mapping.schema_mapping.name}:{project_obj.schema_mapping.version}"
                            if project_obj.schema_mapping
                            else None
                        ),
                        pop=project_obj.pop,
                        stars_number=project_obj.number_of_stars,
                        forked_from=(
                            f"{project_obj.forked_from_mapping.namespace}/{project_obj.forked_from_mapping.name}:{project_obj.forked_from_mapping.tag}"
                            if project_obj.forked_from_mapping
                            else None
                        ),
                    )
                    anno_results.append(annot)

            found_dict = {f"{r.namespace}/{r.name}:{r.tag}": r for r in anno_results}
            end_results = [found_dict.get(project) for project in registry_paths]

            return_len = len(anno_results)
            return AnnotationList(
                count=return_len,
                limit=len(registry_paths),
                offset=0,
                results=end_results,
            )

        else:
            return self.get_by_rp(registry_paths, admin)

    def get_projects_list(
        self,
        namespace: str = None,
        search_str: str = None,
        admin: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET,
        order_by: str = "update_date",
        order_desc: bool = False,
        filter_by: Literal["submission_date", "last_update_date"] | None = None,
        filter_start_date: str | None = None,
        filter_end_date: str | None = None,
        pep_type: Literal["pep", "pop"] | None = None,
    ) -> list[RegistryPath]:
        """Retrieve a list of registry paths matching the given filters.

        Lightweight alternative to get() — returns only registry paths, no annotation data.

        Args:
            namespace: Namespace to restrict the search to.
            search_str: String to search in name, tag, and description.
            admin: Namespace(s) with admin/private access.
            limit: Maximum number of results.
            offset: Number of results to skip.
            order_by: Sort field — "name", "update_date", "submission_date", or "stars".
            order_desc: Sort in descending order if True.
            filter_by: Date field for range filter — "submission_date" or "last_update_date".
            filter_start_date: Range start in YYYY/MM/DD format.
            filter_end_date: Range end in YYYY/MM/DD format (default: today).
            pep_type: Restrict to "pep" or "pop" (default: all).

        Returns:
            List of RegistryPath objects.
        """
        _LOGGER.info(
            f"Running project search: (namespace: {namespace}, query: {search_str}."
        )

        if admin is None:
            admin = []
        statement = select(Projects.namespace, Projects.name, Projects.tag)

        statement = self._add_condition(
            statement,
            namespace=namespace,
            search_str=search_str,
            admin_list=admin,
        )
        statement = self._add_date_filter_if_provided(
            statement, filter_by, filter_start_date, filter_end_date
        )
        statement = self._add_order_by_keyword(statement, by=order_by, desc=order_desc)
        statement = statement.limit(limit).offset(offset)
        if pep_type:
            statement = statement.where(Projects.pop.is_(pep_type == "pop"))

        results_list = []
        with Session(self._sa_engine) as session:
            results = session.execute(statement)

            for result in results:
                results_list.append(
                    RegistryPath(namespace=result[0], name=result[1], tag=result[2])
                )
        return results_list
