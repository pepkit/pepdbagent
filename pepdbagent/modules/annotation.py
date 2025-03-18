import logging
from datetime import datetime
from typing import List, Literal, Optional, Union

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session
from sqlalchemy.sql.selectable import Select

from pepdbagent.const import (
    DEFAULT_LIMIT,
    DEFAULT_OFFSET,
    DEFAULT_TAG,
    LAST_UPDATE_DATE_KEY,
    PKG_NAME,
    SUBMISSION_DATE_KEY,
)
from pepdbagent.db_utils import BaseEngine, Projects
from pepdbagent.exceptions import FilterError, ProjectNotFoundError, RegistryPathError
from pepdbagent.models import AnnotationList, AnnotationModel, RegistryPath
from pepdbagent.utils import convert_date_string_to_date, registry_path_converter, tuple_converter

_LOGGER = logging.getLogger(PKG_NAME)


class PEPDatabaseAnnotation:
    """
    Class that represents project Annotations in the Database.

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
        namespace: str = None,
        name: str = None,
        tag: str = None,
        query: str = None,
        admin: Union[List[str], str] = None,
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET,
        order_by: str = "update_date",
        order_desc: bool = False,
        filter_by: Optional[Literal["submission_date", "last_update_date"]] = None,
        filter_start_date: Optional[str] = None,
        filter_end_date: Optional[str] = None,
        pep_type: Optional[Literal["pep", "pop"]] = None,
    ) -> AnnotationList:
        """
        Get project annotations.

        There is 5 scenarios how to get project or projects annotations:
            - provide name, namespace and tag. Return: project annotations of exact provided PK(namespace, name, tag)
            - provide only namespace. Return: list of projects annotations in specified namespace
            - Nothing is provided. Return: list of projects annotations in all database
            - provide query. Return: list of projects annotations find in database that have query pattern.
            - provide query and namespace. Return:  list of projects annotations find in specific namespace
                that have query pattern.
        :param namespace: Namespace
        :param name: Project name
        :param tag: tag
        :param query: query (search string): Pattern of name, tag or description
        :param admin: admin name (namespace), or list of namespaces, where user is admin
        :param limit: return limit
        :param offset: return offset
        :param order_by: sort the result-set by the information
            Options: ["name", "update_date", "submission_date"]
            [Default: update_date]
        :param order_desc: Sort the records in descending order. [Default: False]
        :param filter_by: data to use filter on.
            Options: ["submission_date", "last_update_date"]
            [Default: filter won't be used]
        :param filter_start_date: Filter start date. Format: "YYYY/MM/DD"
        :param filter_end_date: Filter end date. Format: "YYYY/MM/DD". if None: present date will be used
        :param pep_type: Get pep with specified type. Options: ["pep", "pop"]. Default: None, get all peps
        :return: pydantic model: AnnotationList
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
            raise ValueError(f"pep_type should be one of ['pep', 'pop'], got {pep_type}")

        return AnnotationList(
            limit=limit,
            offset=offset,
            count=self._count_projects(
                namespace=namespace,
                search_str=query,
                admin=admin,
                filter_by=filter_by,
                filter_end_date=filter_end_date,
                filter_start_date=filter_start_date,
                pep_type=pep_type,
            ),
            results=self._get_projects(
                namespace=namespace,
                search_str=query,
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
        registry_paths: Union[List[str], str],
        admin: Union[List[str], str] = None,
    ) -> AnnotationList:
        """
        Get project annotations by providing registry_path or list of registry paths.
        :param registry_paths: registry path string or list of registry paths
        :param admin: list of namespaces where user is admin
                :return: pydantic model: AnnotationReturnModel(
            limit:
            offset:
            count:
            result: List [AnnotationModel])
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
                    single_return = self._get_single_annotation(namespace, name, tag, admin)
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
        admin: Union[List[str], str] = None,
    ) -> Union[AnnotationModel, None]:
        """
        Retrieving project annotation dict by specifying project name
        :param namespace: project registry_path - will return dict of project annotations
        :param name: project name in database
        :param tag: tag of the projects
        :param admin: string or list of admins [e.g. "Khoroshevskyi", or ["doc_adin","Khoroshevskyi"]]
        :return: pydantic Annotation Model of annotations of current project
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
                raise ProjectNotFoundError(f"Project '{namespace}/{name}:{tag}' was not found.")

    def _count_projects(
        self,
        namespace: str = None,
        search_str: str = None,
        admin: Union[str, List[str]] = None,
        filter_by: Optional[Literal["submission_date", "last_update_date"]] = None,
        filter_start_date: Optional[str] = None,
        filter_end_date: Optional[str] = None,
        pep_type: Optional[Literal["pep", "pop"]] = None,
    ) -> int:
        """
        Count projects. [This function is related to _find_projects]

        :param namespace: namespace where to search for a project
        :param search_str: search string. will be searched in name, tag and description information
        :param admin: string or list of admins [e.g. "Khoroshevskyi", or ["doc_adin","Khoroshevskyi"]]
        :param filter_by: data to use filter on.
            Options: ["submission_date", "last_update_date"]
            [Default: filter won't be used]
        :param filter_start_date: Filter start date. Format: "YYYY:MM:DD"
        :param filter_end_date: Filter end date. Format: "YYYY:MM:DD". if None: present date will be used
        :param pep_type: Get pep with specified type. Options: ["pep", "pop"]. Default: None, get all peps

        :return: number of found project in specified namespace
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
        result = self._pep_db_engine.session_execute(statement).first()

        try:
            return result[0]
        except IndexError:
            return 0

    def _get_projects(
        self,
        namespace: str = None,
        search_str: str = None,
        admin: Union[str, List[str]] = None,
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET,
        order_by: str = "update_date",
        order_desc: bool = False,
        filter_by: Optional[Literal["submission_date", "last_update_date"]] = None,
        filter_start_date: Optional[str] = None,
        filter_end_date: Optional[str] = None,
        pep_type: Optional[Literal["pep", "pop"]] = None,
    ) -> List[AnnotationModel]:
        """
        Get projects by providing search string.

        :param namespace: namespace where to search for a project
        :param search_str: search string that has to be found in the name or tag
        :param admin: True, if user is admin of the namespace [Default: False]
        :param limit: limit of return results
        :param offset: number of results off set (that were already showed)
        :param order_by: sort the result-set by the information
            Options: ["update_date", "name",  "submission_date", "stars"]
            [Default: "update_date"]
        :param order_desc: Sort the records in descending order. [Default: False]
        :param filter_by: data to use filter on.
            Options: ["submission_date", "last_update_date"]
            [Default: filter won't be used]
        :param filter_start_date: Filter start date. Format: "YYYY:MM:DD"
        :param filter_end_date: Filter end date. Format: "YYYY:MM:DD". if None: present date will be used
        :param pep_type: Get pep with specified type. Options: ["pep", "pop"]. Default: None, get all peps
        :return: list of found projects with their annotations.
        """
        _LOGGER.info(f"Running annotation search: (namespace: {namespace}, query: {search_str}.")

        if admin is None:
            admin = []
        statement = select(Projects)

        statement = self._add_condition(
            statement,
            namespace=namespace,
            search_str=search_str,
            admin_list=admin,
        )
        statement = self._add_date_filter_if_provided(
            statement, filter_by, filter_start_date, filter_end_date
        )
        statement = statement
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
    def _add_order_by_keyword(
        statement: Select, by: str = "update_date", desc: bool = False
    ) -> Select:
        """
        Add order by clause to sqlalchemy statement

        :param statement: sqlalchemy representation of a SELECT statement.
        :param by: sort the result-set by the information
            Options: ["name", "update_date", "submission_date", "stars"]
            [Default: "update_date"]
        :param desc: Sort the records in descending order. [Default: False]
        :return: sqlalchemy representation of a SELECT statement with order by keyword
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
        admin_list: Union[str, List[str]] = None,
    ) -> Select:
        """
        Add where clause to sqlalchemy statement (in project search)

        :param statement: sqlalchemy representation of a SELECT statement.
        :param namespace: project namespace sql:(where namespace = "")
        :param search_str: search string that has to be found in the name or tag
        :param admin_list: list or string of admin rights to namespace
        :return: sqlalchemy representation of a SELECT statement with where clause.
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

        statement = statement.where(
            or_(Projects.private.is_(False), Projects.namespace.in_(admin_list))
        )

        return statement

    @staticmethod
    def _add_date_filter_if_provided(
        statement: Select,
        filter_by: Optional[Literal["submission_date", "last_update_date"]],
        filter_start_date: Optional[str],
        filter_end_date: Optional[str] = None,
    ):
        """
        Add filter to where clause to sqlalchemy statement (in project search)

        :param statement: sqlalchemy representation of a SELECT statement with where clause
        :param filter_by: data to use filter on.
            Options: ["submission_date", "last_update_date"]
        :param filter_start_date: Filter start date. Format: "YYYY:MM:DD"
        :param filter_end_date: Filter end date. Format: "YYYY:MM:DD". if None: present date will be used
        :return: sqlalchemy representation of a SELECT statement with where clause with added filter
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
                _LOGGER.warning("filter_start_date was not provided, skipping filter...")
            return statement

    def get_project_number_in_namespace(
        self,
        namespace: str,
        admin: Union[str, List[str]] = None,
    ) -> int:
        """
        Get number of found projects by providing search string.

        :param namespace: namespace where to search for a project
        :param admin: True, if user is admin of the namespace [Default: False]
        :return Integer: number of projects in the namepsace
        """
        if admin is None:
            admin = []
        statement = (
            select(func.count()).select_from(Projects).where(Projects.namespace == namespace)
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
        registry_paths: List[str],
        admin: Union[str, List[str]] = None,
    ) -> AnnotationList:
        """
        Get project annotations by providing list of registry paths.

        :param registry_paths: registry path string or list of registry paths
        :param admin: list of namespaces where user is admin
        :return: pydantic model: AnnotationReturnModel(
            limit:
            offset:
            count:
            result: List [AnnotationModel])
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

            statement = select(Projects).where(or_(*or_statement_list))
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
        admin: Union[str, List[str]] = None,
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET,
        order_by: str = "update_date",
        order_desc: bool = False,
        filter_by: Optional[Literal["submission_date", "last_update_date"]] = None,
        filter_start_date: Optional[str] = None,
        filter_end_date: Optional[str] = None,
        pep_type: Optional[Literal["pep", "pop"]] = None,
    ) -> List[RegistryPath]:
        """
        Retrieve a list of projects by providing a search string.
        This function serves as a lightweight version of the full 'get' function,
        returning only a list of registry paths without annotations.
        It is designed for use cases where a large list of projects is needed with minimal processing time.

        :param namespace: namespace where to search for a project
        :param search_str: search string that has to be found in the name or tag
        :param admin: True, if user is admin of the namespace [Default: False]
        :param limit: limit of return results
        :param offset: number of results off set (that were already showed)
        :param order_by: sort the result-set by the information
            Options: ["name", "update_date", "submission_date", "stars"]
            [Default: "update_date"]
        :param order_desc: Sort the records in descending order. [Default: False]
        :param filter_by: data to use filter on.
            Options: ["submission_date", "last_update_date"]
            [Default: filter won't be used]
        :param filter_start_date: Filter start date. Format: "YYYY:MM:DD"
        :param filter_end_date: Filter end date. Format: "YYYY:MM:DD". if None: present date will be used
        :param pep_type: Get pep with specified type. Options: ["pep", "pop"]. Default: None, get all peps
        :return: list of found projects with their annotations.
        """
        _LOGGER.info(f"Running project search: (namespace: {namespace}, query: {search_str}.")

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
