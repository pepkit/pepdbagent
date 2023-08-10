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
from pepdbagent.db_utils import BaseEngine, PEPGroup
from pepdbagent.exceptions import GroupUniqueNameError, GroupNotFoundError
from pepdbagent.models import GroupListInfo, GroupInfo, GroupUpdateModel
from pepdbagent.utils import create_digest, registry_path_converter


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
            - provide namespace and name. Return: project annotations of exact provided PK(namespace, name)
            - provide only namespace. Return: list of groups in specified namespace
            - Nothing is provided. Return: list of groups in all database
            - provide query. Return: list of groups find in database that have query pattern (search will be done based on name and description)
            - provide query and namespace. Return: list of groups find in specific namespace
                that have query pattern.
        :param namespace: Namespace
        :param name: Project name
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
            count=self._count_projects(namespace=namespace, search_str=query, admin=admin),
            results=self._get_projects(
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

        :param namespace:
        :param name:
        :param admin:
        :return:
        """
        statement = select(PEPGroup)
        statement = statement.where(PEPGroup.namespace == namespace).where(PEPGroup.name == name)
        statement = statement.where(
            or_(PEPGroup.private.is_(False), PEPGroup.namespace.in_(admin))
        )
        with Session(self._sa_engine) as session:
            query_result = session.scalar(statement)

        return GroupInfo(
            namespace=query_result.namespace,
            name=query_result.name,
            private=query_result.private,
            number_of_projects=0,
            description=query_result.description,
            last_update_date=query_result.last_update_date,
        )

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

        :param namespace:
        :param name:
        :param project_namespace:
        :param project_name:
        :param project_tag:
        :return:
        """
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

        :param namespace:
        :param name:
        :param project_namespace:
        :param project_name:
        :param project_tag:
        :return:
        """
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
