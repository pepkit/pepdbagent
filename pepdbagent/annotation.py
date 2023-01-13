from typing import Union, List
import logging

from .base import BaseConnection
from .const import (
    DEFAULT_LIMIT,
    DEFAULT_OFFSET,
    DEFAULT_TAG,
    NAMESPACE_COL,
    NAME_COL,
    TAG_COL,
    PRIVATE_COL,
    PROJ_COL,
    N_SAMPLES_COL,
    SUBMISSION_DATE_COL,
    LAST_UPDATE_DATE_COL,
    DIGEST_COL,
    DB_TABLE_NAME
)
from .utils import tuple_converter, registry_path_converter

from .models import AnnotationModel, AnnotationReturnModel
from .exceptions import RegistryPathError

_LOGGER = logging.getLogger("pepdbagent")


class PEPDatabaseAnnotation:
    """
    Class that represents project Annotations in the Database.

    While using this class, user can retrieve all necessary metadata about PEPs
    """

    def __init__(self, con: BaseConnection):
        """
        :param con: Connection to db represented by BaseConnection class object
        """
        self.con = con

    def get(
        self,
        namespace: str = None,
        name: str = None,
        tag: str = None,
        query: str = "",
        admin: Union[List[str], str] = None,
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET
    ) -> AnnotationReturnModel:
        """

        :param namespace:
        :param name:
        :param tag:
        :param query:
        :param admin:
        :param limit:
        :param offset:
        :return:
        """
        ...
        if all([namespace, name, tag]):
            found_annotations = list(self._get_single_annotation(
                    namespace=namespace,
                    name=name,
                    tag=tag,
                    admin=admin,
                ))
            return AnnotationReturnModel(
                count=len(found_annotations),
                limit=1,
                offset=1,
                result=found_annotations,
            )
        return AnnotationReturnModel(
            limit=limit,
            offset=offset,
            count=self._count_projects(
                namespace=namespace, search_str=query, admin=admin
            ),
            result=self._get_projects(
                namespace=namespace,
                search_str=query,
                admin=admin,
                offset=offset,
                limit=limit,
            ),
        )

    def get_by_rp(
        self,
        registry_paths: Union[List[str], str],
        admin: Union[List[str], str] = None,
    ) -> AnnotationReturnModel:
        """

        :param registry_paths: registry path string or list of registry paths
        :param admin: list of namespaces where user is admin
        :return:
        """
        if isinstance(registry_paths, list):
            anno_results = []
            for path in registry_paths:
                try:
                    namespace, name, tag = registry_path_converter(path)
                except RegistryPathError as err:
                    _LOGGER.error(str(err), registry_paths)
                    continue
                single_return = self._get_single_annotation(namespace, name, tag, admin)
                if single_return:
                    anno_results.append(single_return)
            return_len = len(anno_results)
            return AnnotationReturnModel(
                count=return_len,
                limit=return_len,
                offset=len(registry_paths),
                result=anno_results,
            )

        else:
            try:
                namespace, name, tag = registry_path_converter(registry_paths)
            except RegistryPathError as err:
                _LOGGER.error(str(err), registry_paths)
                return AnnotationReturnModel()
            return self.get(namespace=namespace, name=name, tag=tag, admin=admin)

    def _get_single_annotation(
        self,
        namespace: str = None,
        name: str = None,
        tag: str = None,
        admin: Union[List[str], str] = None,
    ) -> Union[AnnotationModel, None]:
        """
        Retrieving project annotation dict by specifying project name
        :param namespace: project registry_path - will return dict of project annotations
        :param name: project name in database
        :param tag: tag of the projects
        :param admin: list of namespaces where user is admin
        :return: pydantic Annotation Model of annotations of current project
        """
        admin_tuple = tuple_converter(admin)
        sql_q = f"""
                select 
                    {NAMESPACE_COL},
                    {NAME_COL},
                    {TAG_COL},
                    {PRIVATE_COL},
                    {PROJ_COL}->>'description',
                    {N_SAMPLES_COL},
                    {SUBMISSION_DATE_COL},
                    {LAST_UPDATE_DATE_COL},
                    {DIGEST_COL}
                        from {DB_TABLE_NAME}
                """

        if tag is None:
            tag = DEFAULT_TAG

        if name:
            sql_q = f""" {sql_q} where {NAME_COL}=%s and {NAMESPACE_COL}=%s and {TAG_COL}=%s 
                                and ({PRIVATE_COL} is %s or {NAMESPACE_COL} in %s );"""
            found_prj = self.con.run_sql_fetchone(sql_q, name, namespace, tag, False, admin_tuple)

        else:
            _LOGGER.error(
                "You haven't provided name, digest nor id! Execution is unsuccessful"
            )
            _LOGGER.info("Files haven't been downloaded, returning empty dict")
            return AnnotationModel()

        _LOGGER.info(f"Project has been found!")
        if len(found_prj) > 0:
            annot = AnnotationModel(
                namespace=found_prj[0],
                name=found_prj[1],
                tag=found_prj[2],
                is_private=found_prj[3],
                description=found_prj[4],
                number_of_samples=found_prj[5],
                submission_date=str(found_prj[6]),
                last_update_date=str(found_prj[7]),
                digest=found_prj[8],
            )
            return annot
        else:
            _LOGGER.error(f"Project '{namespace}/{name}:{tag}' was not found.")

        return None

    def _count_projects(
        self,
        namespace: str,
        search_str: str = "",
        admin: Union[str, List[str]] = False,
    ) -> int:
        """
        Get total number of found projects. [This function is related to _find_projects]
        :param namespace: namespace where to search for a project
        :param search_str: search string. will be searched in name and description information
        :param admin: True, if user is admin for this namespace
        :return: number of found project in specified namespace
        """
        search_str = f"%%{search_str}%%"
        admin_tuple = tuple_converter(admin)
        if namespace:
            and_namespace_sql = f"""AND {NAMESPACE_COL} = %s"""
            namespace = (namespace,)
        else:
            and_namespace_sql = ""
            namespace = tuple()

        count_sql = f"""
        select count(*)
            from {DB_TABLE_NAME} 
                where ({NAME_COL} ILIKE %s or ({PROJ_COL}->>'description') ILIKE %s) 
                    and ({PRIVATE_COL} is %s or {NAMESPACE_COL} in %s ) {and_namespace_sql};"""
        result = self.con.run_sql_fetchall(
            count_sql, search_str, search_str, False, admin_tuple, *namespace
        )
        try:
            number_of_prj = result[0][0]
        except IndexError:
            number_of_prj = 0
        return number_of_prj

    def _get_projects(
        self,
        namespace: str = None,
        search_str: str = "",
        admin: Union[str, List[str]] = False,
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET,
    ) -> List[AnnotationModel]:
        """
        Search for project inside namespace by providing search string.
        :param namespace: namespace where to search for a project
        :param search_str: search string that has to be found in the name or project description
        :param admin: True, if user is admin of the namespace [Default: False]
        :param limit: limit of return results
        :param offset: number of results off set (that were already showed)
        :return: list of found projects with their annotations.
        """
        search_str = f"%%{search_str}%%"
        admin_tuple = tuple_converter(admin)

        if namespace:
            and_namespace_sql = f"""AND {NAMESPACE_COL} = %s"""
            namespace = (namespace,)
        else:
            and_namespace_sql = ""
            namespace = tuple()

        count_sql = f"""
        select {NAMESPACE_COL}, {NAME_COL}, {TAG_COL}, {N_SAMPLES_COL}, ({PROJ_COL}->>'description'), {DIGEST_COL}, {PRIVATE_COL}, {SUBMISSION_DATE_COL}, {LAST_UPDATE_DATE_COL}
            from {DB_TABLE_NAME} 
                where ({NAME_COL} ILIKE %s or ({PROJ_COL}->>'description') ILIKE %s) 
                    and ({PRIVATE_COL} is %s or {NAMESPACE_COL} in %s ) {and_namespace_sql}
                        LIMIT %s OFFSET %s;
        """
        results = self.con.run_sql_fetchall(
            count_sql, search_str, search_str, False, admin_tuple, *namespace, limit, offset
        )
        results_list = []
        try:
            for res in results:
                results_list.append(
                    AnnotationModel(
                        namespace=res[0],
                        name=res[1],
                        tag=res[2],
                        number_of_samples=res[3],
                        description=res[4],
                        digest=res[5],
                        is_private=res[6],
                        last_update_date=str(res[8]),
                        submission_date=str(res[7]),
                    )
                )
        except KeyError:
            results_list = []

        return results_list