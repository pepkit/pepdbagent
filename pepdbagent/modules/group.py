import datetime
import json
import logging
from typing import Union, List, NoReturn, Optional, Tuple

import peppy
from sqlalchemy import Engine, and_, delete, insert, or_, select, update, func
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlalchemy.orm import Session
from sqlalchemy import Select

from peppy.const import SAMPLE_RAW_DICT_KEY, SUBSAMPLE_RAW_LIST_KEY, CONFIG_KEY

from pepdbagent.const import *
from pepdbagent.db_utils import BaseEngine, PEPGroup, GroupProjectAssociation
from pepdbagent.exceptions import GroupUniqueNameError, GroupNotFoundError
from pepdbagent.models import GroupListInfo, GroupInfo, GroupUpdateModel, ProjectRegistryPath
from pepdbagent.utils import tuple_converter
from pepdbagent.modules.project import PEPDatabaseProject


_LOGGER = logging.getLogger(PKG_NAME)


class PEPDatabaseGroup:
    """
    Class that represents Group of projects in Database.

    While using this class, user can create, delete, update groups and add, remove projects from it.
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
        query: str = None,
        admin: Union[List[str], Tuple[str]] = tuple(),
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET,
    ) -> GroupListInfo:
        """
        Get group of peps. (* This method doesn't retrieve actual PEPs)

        There is 5 scenarios how to get Group of PEPs:
            - provide namespace and name. Return: groups of exact provided PK(namespace, name)
            - provide only namespace. Return: list of groups in specified namespace
            - Nothing is provided. Return: list of groups in all database
            - provide query. Return: list of groups find in database that have query pattern (search will be done based on name and description)
            - provide query and namespace. Return: list of groups find in specific namespace
                that have query pattern.
        :param namespace: Namespace
        :param name: Group name
        :param query: query (search string): Pattern of name, or description
        :param admin: admin name (namespace), or list of namespaces, where user is admin
        :param limit: return limit
        :param offset: return offset
        :param order_by: sort the result-set by the information
            Options: ["name", "update_date", "submission_date"]
            [Default: update_date]
        :param order_desc: Sort the records in descending order. [Default: False]
        :return: pydantic model: GroupListInfo
        """
        if all([namespace, name]):
            found_annotation = [
                self._get_single_group(
                    namespace=namespace,
                    name=name,
                    admin=admin,
                )
            ]
            return GroupListInfo(
                count=len(found_annotation),
                limit=1,
                offset=0,
                results=found_annotation,
            )
        return GroupListInfo(
            limit=limit,
            offset=offset,
            count=self._count_groups(namespace=namespace, search_str=query, admin=admin),
            results=self._get_groups(
                namespace=namespace,
                search_str=query,
                admin=admin,
                offset=offset,
                limit=limit,
            ),
        )

    def _get_single_group(
        self, namespace: str, name: str, admin: Union[tuple, list] = tuple([])
    ) -> GroupInfo:
        """
        Get one group with it's projects

        :param namespace: group namesapce
        :param name: group name
        :param admin: admin list [tuple or list]
        :return:
        """
        statement = select(PEPGroup)
        statement = statement.where(PEPGroup.namespace == namespace).where(PEPGroup.name == name)
        statement = statement.where(
            or_(PEPGroup.private.is_(False), PEPGroup.namespace.in_(admin))
        )

        with Session(self._sa_engine) as session:
            query_result = session.scalar(statement)

            number_of_projects = len([kk.project for kk in query_result.projects])
            project_list = []
            for prj_list in query_result.projects:
                project_list.append(
                    ProjectRegistryPath(
                        namespace=prj_list.project.namespace,
                        name=prj_list.project.name,
                        tag=prj_list.project.tag,
                        private=prj_list.project.private,
                    )
                )
            return GroupInfo(
                namespace=query_result.namespace,
                name=query_result.name,
                private=query_result.private,
                number_of_projects=number_of_projects,
                projects=project_list,
                description=query_result.description,
                last_update_date=query_result.last_update_date,
            )

    def _get_groups(
        self,
        namespace: str = None,
        search_str: str = None,
        admin: Union[list, str] = tuple(),
        offset: int = 0,
        limit: int = 50,
    ):
        """
        Get list of groups by providing search string (optional), and specifying namespace (optional)

        :param namespace: Groups namespace
        :param search_str: Search string that has to be found in the group name or description
        :param admin: list or tuple of admin rights to namespace
        :param limit: limit of return results
        :param offset: number of results off set (that were already showed)
        :return: ????
        """
        _LOGGER.info(f"Running annotation search: (namespace: {namespace}, query: {search_str}.")

        if admin is None:
            admin = []
        statement = select(PEPGroup)

        statement = self._add_condition(
            statement,
            namespace=namespace,
            search_str=search_str,
            admin_list=admin,
        )
        # statement = self._add_order_by_keyword(statement, by=order_by, desc=order_desc)
        statement = statement.limit(limit).offset(offset)

        # query_results = self._pep_db_engine.session_execute(statement).all()
        with Session(self._sa_engine) as session:
            results_list = []
            for query_result in session.scalars(statement):
                number_of_projects = len([kk.project for kk in query_result.projects])
                project_list = []
                for prj_list in query_result.projects:
                    project_list.append(
                        ProjectRegistryPath(
                            namespace=prj_list.project.namespace,
                            name=prj_list.project.name,
                            tag=prj_list.project.tag,
                            private=prj_list.project.private,
                        )
                    )

                results_list.append(
                    GroupInfo(
                        namespace=query_result.namespace,
                        name=query_result.name,
                        private=query_result.private,
                        number_of_projects=number_of_projects,
                        projects=project_list,
                        description=query_result.description,
                        last_update_date=query_result.last_update_date,
                    )
                )
            return results_list

    def _count_groups(
        self,
        namespace: str = None,
        search_str: str = None,
        admin: Union[list, str] = tuple(),
    ) -> int:
        """
        Count groups using search pattern and namepsace. [This function is related to _find_groups]

        :param namespace: namespace where to search for groups
        :param search_str: search string. will be searched in name, tag and description information
        :param admin: string or list of admins [e.g. "Khoroshevskyi", or ["doc_adin","Khoroshevskyi"]]
        :return: Number of found groups
        """

        if admin is None:
            admin = []
        statement = select(func.count()).select_from(PEPGroup)
        statement = self._add_condition(
            statement,
            namespace=namespace,
            search_str=search_str,
            admin_list=admin,
        )
        result = self._pep_db_engine.session_execute(statement).first()

        try:
            return result[0]
        except IndexError:
            return 0

    def create(
        self, namespace: str, name: str, private: bool, description: Optional[str] = ""
    ) -> None:
        """
        Create new group

        :param namespace: User, or organization that creates new group
        :param name: name of the group
        :param private: is group private?
        :param description: description of the group
        :return: None
        """

        new_group = PEPGroup(
            namespace=namespace,
            name=name,
            private=private,
            description=description,
        )
        try:
            with Session(self._sa_engine) as session:
                session.add(new_group)
                session.commit()

            return None

        except IntegrityError:
            raise GroupUniqueNameError(msg="Group name already exists, change group name!")

    def add_project(
        self,
        namespace: str,
        name: str,
        project_namespace: str,
        project_name: str,
        project_tag: Optional[str] = DEFAULT_TAG,
    ) -> None:
        """
        Add project to the group

        :param namespace: group namespace
        :param name: group name
        :param project_namespace: project namespace
        :param project_name: project name
        :param project_tag: project tag
        :return: None (If project was added successfully)
        """

        group_id = self._get_group_id(namespace=namespace, name=name)
        project_id = PEPDatabaseProject(self._pep_db_engine)._get_project_id(
            project_namespace, project_name, project_tag
        )
        new_assosiation_raw = GroupProjectAssociation(group_id=group_id, project_id=project_id)

        with Session(self._sa_engine) as session:
            session.add(new_assosiation_raw)
            session.commit()

        return None

    def remove_project(
        self,
        namespace: str,
        name: str,
        project_namespace: str,
        project_name: str,
        project_tag: Optional[str] = DEFAULT_TAG,
    ) -> None:
        """
        Remove project from the group

        :param namespace: group namespace
        :param name: group name
        :param project_namespace: project namespace
        :param project_name: project name
        :param project_tag: project tag
        :return: None (If project was added successfully)
        """

        group_id = self._get_group_id(namespace=namespace, name=name)
        project_id = PEPDatabaseProject(self._pep_db_engine)._get_project_id(
            project_namespace, project_name, project_tag
        )
        statement = delete(GroupProjectAssociation).where(
            and_(
                GroupProjectAssociation.group_id == group_id,
                GroupProjectAssociation.project_id == project_id,
            )
        )
        with Session(self._sa_engine) as session:
            session.execute(statement)
            session.commit()

        return None

    def delete(
        self,
        namespace: str,
        name: str,
    ) -> None:
        """
        Delete group with all associations

        :param namespace: group namespace
        :param name: group name
        :return: None
        """
        if not self.exists(namespace=namespace, name=name):
            raise GroupNotFoundError(
                f"Can't delete not existing group: namespace : '{namespace}', name: '{name}'."
            )
        with self._sa_engine.begin() as conn:
            conn.execute(
                delete(PEPGroup).where(
                    and_(
                        PEPGroup.namespace == namespace,
                        PEPGroup.name == name,
                    )
                )
            )
        return None

    def update(
        self, namespace: str, name: str, update_dict: Union[GroupUpdateModel, dict]
    ) -> None:
        """
        Update partial parts of the group in db

        :param update_dict: dict with update key->values. Dict structure:
            {
                    private: Optional[bool]
                    description: Optional[str]
                    name: Optional[str]
            }
        :param namespace: group namespace
        :param name: group name
        :return: None
        """
        if isinstance(update_dict, dict):
            update_dict = GroupUpdateModel(**update_dict)
        update_final = update_dict.dict(exclude_unset=True, exclude_none=True)

        statement = select(PEPGroup).where(
            and_(PEPGroup.namespace == namespace, PEPGroup.name == name)
        )

        with Session(self._sa_engine) as session:
            found_prj = session.scalars(statement).one()

            if found_prj:
                _LOGGER.debug(f"Project has been found: {found_prj.namespace}, {found_prj.name}")

                for k, v in update_final.items():
                    if getattr(found_prj, k) != v:
                        setattr(found_prj, k, v)

            session.commit()

    def exists(self, namespace: str, name: str) -> bool:
        """
        Get group id by providing namespace and name

        :param namespace: Namespace of the group
        :param name: Name of the group
        :return: True if project exists
        """
        return True if self._get_group_id(namespace, name) else False

    def _get_group_id(self, namespace: str, name: str) -> Union[int, None]:
        """
        Get group id by providing namespace and name

        :param namespace: Namespace of the group
        :param name: Name of the group
        :return: groups ID
        """
        statement = select(PEPGroup.id).where(
            and_(PEPGroup.namespace == namespace, PEPGroup.name == name)
        )
        with Session(self._sa_engine) as session:
            result = session.execute(statement).one_or_none()

        if result:
            return result[0]
        return None

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
                PEPGroup.name.ilike(sql_search_str),
                PEPGroup.description.ilike(sql_search_str),
            )
            statement = statement.where(search_query)
        if namespace:
            statement = statement.where(PEPGroup.namespace == namespace)

        statement = statement.where(
            or_(PEPGroup.private.is_(False), PEPGroup.namespace.in_(admin_list))
        )

        return statement
