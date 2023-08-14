import datetime
import json
import logging
from typing import Union, List, NoReturn, Optional

import peppy
from sqlalchemy import Engine, and_, delete, insert, or_, select, update
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlalchemy.orm import Session
from sqlalchemy import Select

from peppy.const import SAMPLE_RAW_DICT_KEY, SUBSAMPLE_RAW_LIST_KEY, CONFIG_KEY

from pepdbagent.const import *
from pepdbagent.db_utils import BaseEngine, PEPGroup, GroupProjectAssociation
from pepdbagent.exceptions import GroupUniqueNameError, GroupNotFoundError
from pepdbagent.models import GroupListInfo, GroupInfo, GroupUpdateModel
from pepdbagent.utils import create_digest, registry_path_converter
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
        admin: Union[List[str], str] = tuple(),
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

        # TODO: add list of project to return object
        with Session(self._sa_engine) as session:
            query_result = session.scalar(statement)

            number_of_projects = len([kk.project for kk in query_result.projects])
            return GroupInfo(
                namespace=query_result.namespace,
                name=query_result.name,
                private=query_result.private,
                number_of_projects=number_of_projects,
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
        ...
        # 1. Get Groups... what doest it mean? I should rethink this method...

    def _count_groups(
        self,
        namespace: str = None,
        search_str: str = None,
        admin: Union[list, str] = tuple(),
        offset: int = 0,
        limit: int = 50,
    ) -> int:
        """
        Count groups using search pattern and namepsace. [This function is related to _find_groups]

        :param namespace: namespace where to search for groups
        :param search_str: search string. will be searched in name, tag and description information
        :param admin: string or list of admins [e.g. "Khoroshevskyi", or ["doc_adin","Khoroshevskyi"]]
        :return: Number of found groups
        """
        ...
        # 1. Get Groups... what doest it mean? I should rethink this method...

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
        # 1. get project id,
        # 2. get group id,
        # 3. insert id to the association table:D
        pass

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
        # 1. get project id,
        # 2. get group id,
        # 3. delete from association table
        pass

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
                f"Can't delete unexciting group: namespace : '{namespace}', name: '{name}'."
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

    def update(self, namespace: str, name: str, update_dict: GroupUpdateModel) -> None:
        """

        :param namespace:
        :param name:
        :param update_dict:
        :return:
        """
        pass

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
