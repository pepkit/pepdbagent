import logging
from typing import Union

from sqlalchemy import and_, delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from pepdbagent.const import PKG_NAME
from pepdbagent.db_utils import BaseEngine, Projects, Stars, User
from pepdbagent.exceptions import (
    ProjectAlreadyInFavorites,
    ProjectNotInFavorites,
    UserNotFoundError,
)
from pepdbagent.models import AnnotationList, AnnotationModel

_LOGGER = logging.getLogger(PKG_NAME)


class PEPDatabaseUser:
    """
    Class that represents Project in Database.

    While using this class, user can create, retrieve, delete, and update projects from database
    """

    def __init__(self, pep_db_engine: BaseEngine):
        """
        :param pep_db_engine: pepdbengine object with sa engine
        """
        self._sa_engine = pep_db_engine.engine
        self._pep_db_engine = pep_db_engine

    def create_user(self, namespace: str) -> int:
        """
        Create new user

        :param namespace: user namespace
        :return: user id
        """
        new_user_raw = User(namespace=namespace)

        with Session(self._sa_engine) as session:
            session.add(new_user_raw)
            session.commit()
            user_id = new_user_raw.id
        return user_id

    def get_user_id(self, namespace: str) -> Union[int, None]:
        """
        Get user id using username

        :param namespace: user namespace
        :return: user id
        """
        statement = select(User.id).where(User.namespace == namespace)
        with Session(self._sa_engine) as session:
            result = session.execute(statement).one_or_none()

        if result:
            return result[0]
        return None

    def add_project_to_favorites(
        self, namespace: str, project_namespace: str, project_name: str, project_tag: str
    ) -> None:
        """
        Add project to favorites

        :param namespace: namespace of the user
        :param project_namespace: namespace of the project
        :param project_name: name of the project
        :param project_tag: tag of the project
        :return: None
        """

        user_id = self.get_user_id(namespace)

        if not user_id:
            user_id = self.create_user(namespace)

        try:
            with Session(self._sa_engine) as session:
                project_mapping = session.scalar(
                    select(Projects).where(
                        and_(
                            Projects.namespace == project_namespace,
                            Projects.name == project_name,
                            Projects.tag == project_tag,
                        )
                    )
                )

                new_favorites_raw = Stars(user_id=user_id, project_id=project_mapping.id)

                session.add(new_favorites_raw)
                project_mapping.number_of_stars += 1
                session.commit()
        except IntegrityError:
            raise ProjectAlreadyInFavorites()
        return None

    def remove_project_from_favorites(
        self, namespace: str, project_namespace: str, project_name: str, project_tag: str
    ) -> None:
        """
        Remove project from favorites

        :param namespace: namespace of the user
        :param project_namespace: namespace of the project
        :param project_name: name of the project
        :param project_tag: tag of the project
        :return: None
        """
        _LOGGER.debug(
            f"Removing project {project_namespace}/{project_name}:{project_tag} from favorites in {namespace}"
        )

        user_id = self.get_user_id(namespace)

        with Session(self._sa_engine) as session:
            project_mapping = session.scalar(
                select(Projects).where(
                    and_(
                        Projects.namespace == project_namespace,
                        Projects.name == project_name,
                        Projects.tag == project_tag,
                    )
                )
            )
            delete_statement = delete(Stars).where(
                and_(
                    Stars.user_id == user_id,
                    Stars.project_id == project_mapping.id,
                )
            )
            project_mapping.number_of_stars -= 1
            result = session.execute(delete_statement)
            session.commit()
            row_count = result.rowcount
        if row_count == 0:
            raise ProjectNotInFavorites(
                f"Project {project_namespace}/{project_name}:{project_tag} is not in favorites for user {namespace}"
            )
        return None

    def get_favorites(self, namespace: str) -> AnnotationList:
        """
        Get list of favorites for user

        :param namespace: namespace of the user
        :return: list of favorite projects with annotations
        """
        _LOGGER.debug(f"Getting favorites for user {namespace}")
        if not self.exists(namespace):
            return AnnotationList(
                count=0,
                limit=0,
                offset=0,
                results=[],
            )
        statement = select(User).where(User.namespace == namespace)
        with Session(self._sa_engine) as session:
            query_result = session.scalar(statement)
            number_of_projects = len([kk.project_mapping for kk in query_result.stars_mapping])
            project_list = []
            for prj_list in query_result.stars_mapping:
                prj = prj_list.project_mapping
                project_list.append(
                    AnnotationModel(
                        namespace=prj.namespace,
                        name=prj.name,
                        tag=prj.tag,
                        is_private=prj.private,
                        number_of_samples=prj.number_of_samples,
                        description=prj.description,
                        last_update_date=str(prj.last_update_date),
                        submission_date=str(prj.submission_date),
                        digest=prj.digest,
                        pep_schema=f"{prj.schema_mapping.schema_mapping.namespace}/{prj.schema_mapping.schema_mapping.name}:{prj.schema_mapping.version}",
                        pop=prj.pop,
                        stars_number=prj.number_of_stars,
                        forked_from=(
                            f"{prj.forked_from_mapping.namespace}/{prj.forked_from_mapping.name}:{prj.forked_from_mapping.tag}"
                            if prj.forked_from_mapping
                            else None
                        ),
                    )
                )
        favorite_prj = AnnotationList(
            count=number_of_projects,
            limit=number_of_projects,
            offset=0,
            results=project_list,
        )
        return favorite_prj

    def exists(
        self,
        namespace: str,
    ) -> bool:
        """
        Check if user exists in the database.

        :param namespace: project namespace
        :return: Returning True if project exist
        """

        statement = select(User.id)
        statement = statement.where(
            and_(
                User.namespace == namespace,
            )
        )
        found_prj = self._pep_db_engine.session_execute(statement).all()

        if len(found_prj) > 0:
            return True
        else:
            return False

    def delete(self, namespace: str) -> None:
        """
        Delete user from the database with all related data

        :param namespace: user namespace
        :return: None
        """
        if not self.exists(namespace):
            raise UserNotFoundError

        with Session(self._sa_engine) as session:
            session.execute(delete(User).where(User.namespace == namespace))
            session.commit()
