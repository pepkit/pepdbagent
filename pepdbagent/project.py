import datetime
import json
from typing import Union, Tuple
import logging
import peppy
import psycopg2
from psycopg2.errors import NotNullViolation, UniqueViolation

from .models import (
    UploadResponse,
    UpdateModel,
    UpdateItems,
)
from .base import BaseConnection
from .const import *
from .utils import create_digest, registry_path_converter
from .exceptions import RegistryPathError

_LOGGER = logging.getLogger("pepdbagent")


class PEPDatabaseProject:
    """
    Class that represents Projects in Database.

    While using this class, user can retrieve projects from database
    """

    def __init__(self, con: BaseConnection):
        """
        :param con: Connection to db represented by BaseConnection class object
        """
        self.con = con

    def get(
        self,
        namespace: str,
        name: str,
        tag: str,
        raw: bool = False,
    ) -> peppy.Project:
        """

        :param namespace:
        :param name:
        :param tag:
        :param raw:
        :return:
        """
        ...


    def get_by_rp(
        self,
        registry_path: str,
        raw: bool = False,
    ) -> peppy.Project:
        """

        :param registry_path:
        :param raw:
        :return:
        """
        ...

    def delete_project(
        self,
        namespace: str = None,
        name: str = None,
        tag: str = None,
    ) -> None:
        cursor = self.con.pg_connection.cursor()
        sql_delete = f"""DELETE FROM {DB_TABLE_NAME} 
        WHERE {NAMESPACE_COL} = %s and {NAME_COL} = %s and {TAG_COL} = %s;"""

        try:
            cursor.execute(sql_delete, (namespace, name, tag))
            _LOGGER.info(f"Project '{namespace}/{name}:{tag} was successfully deleted'")
        except Exception as err:
            _LOGGER.error(f"Error while deleting project. Message: {err}")
        finally:
            cursor.close()
            return None

    def delete_project_by_registry_path(
        self,
        registry_path: str,
    ) -> None:
        try:
            namespace, name, tag = registry_path_converter(registry_path)
        except RegistryPathError as err:
            _LOGGER.error(str(RegistryPathError), registry_path)
            return None
        return self.delete_project(namespace=namespace, name=name, tag=tag)

    def upload(
        self,
        project: peppy.Project,
        namespace: str,
        name: str = None,
        tag: str = None,
        description: str = None,
        is_private: bool = False,
        overwrite: bool = False,
        update_only: bool = False,
    ) -> UploadResponse:
        """
        Upload project to the database.
        Project with the key, that already exists won't be uploaded(but case, when argument
        update is set True)
        :param peppy.Project project: Project object that has to be uploaded to the DB
        :param namespace: namespace of the project (Default: 'other')
        :param name: name of the project (Default: name is taken from the project object)
        :param tag: tag (or version) of the project.
        :param description: project description.
        :param is_private: boolean value if the project should be visible just for user that creates it.
        :param overwrite: if project exists overwrite the project, otherwise upload it.
            [Default: False - project won't be overwritten if it exists in db]
        :param update_only: if project exists overwrite it, otherwise do nothing.  [Default: False]
        """
        cursor = self.con.pg_connection.cursor()
        try:
            if namespace is None:
                namespace = DEFAULT_NAMESPACE
            if tag is None:
                tag = DEFAULT_TAG

            proj_dict = project.to_dict(extended=True)

            if name:
                proj_name = name
            else:
                proj_name = proj_dict["name"]

            proj_dict["description"] = description
            proj_dict["name"] = name

            proj_digest = create_digest(proj_dict)

            number_of_samples = len(project.samples)
            proj_dict = json.dumps(proj_dict)

            if update_only:
                _LOGGER.info(
                    f"Update_only argument is set True. Updating project {proj_name} ..."
                )
                response = self._update_project(
                    project_dict=proj_dict,
                    namespace=namespace,
                    proj_name=proj_name,
                    tag=tag,
                    project_digest=proj_digest,
                    number_of_samples=number_of_samples,
                )
                return response
            else:
                try:
                    _LOGGER.info(f"Uploading {namespace}/{proj_name}:{tag} project...")

                    sql_base = f"""INSERT INTO {DB_TABLE_NAME} 
                    ({NAMESPACE_COL}, {NAME_COL}, {TAG_COL}, {DIGEST_COL}, {PROJ_COL}, {N_SAMPLES_COL}, {PRIVATE_COL}, {SUBMISSION_DATE_COL}, {LAST_UPDATE_DATE_COL})
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING {ID_COL};"""

                    cursor.execute(
                        sql_base,
                        (
                            namespace,
                            proj_name,
                            tag,
                            proj_digest,
                            proj_dict,
                            number_of_samples,
                            is_private,
                            datetime.datetime.now(),
                            datetime.datetime.now(),
                        ),
                    )
                    proj_id = cursor.fetchone()[0]

                    self.con.commit_to_database()
                    cursor.close()
                    _LOGGER.info(
                        f"Project: '{namespace}/{proj_name}:{tag}' was successfully uploaded."
                    )
                    return UploadResponse(
                        registry_path=f"{namespace}/{proj_name}:{tag}",
                        log_stage="upload_project",
                        status="success",
                        info=f"",
                    )

                except UniqueViolation:
                    if overwrite:

                        response = self._update_project(
                            project_dict=proj_dict,
                            namespace=namespace,
                            proj_name=proj_name,
                            tag=tag,
                            project_digest=proj_digest,
                            number_of_samples=number_of_samples,
                        )
                        return response
                    else:
                        _LOGGER.warning(
                            f"Namespace, name and tag already exists. Project won't be uploaded. "
                            f"Solution: Set overwrite value as True (project will be overwritten),"
                            f" or change tag!"
                        )
                        return UploadResponse(
                            registry_path=f"{namespace}/{proj_name}:{tag}",
                            log_stage="upload_project",
                            status="warning",
                            info=f"project already exists! Overwrite argument is False",
                        )

                except NotNullViolation as err:
                    _LOGGER.error(
                        f"Name of the project wasn't provided. Project will not be uploaded. Error: {err}"
                    )
                    return UploadResponse(
                        registry_path=f"{namespace}/{proj_name}:{tag}",
                        log_stage="upload_project",
                        status="failure",
                        info=f"NotNullViolation. Error message: {err}",
                    )

        except psycopg2.Error as e:
            _LOGGER.error(
                f"Error while uploading project. Project hasn't been uploaded! Error: {e}"
            )
            cursor.close()
            return UploadResponse(
                registry_path=f"None",
                log_stage="upload_project",
                status="failure",
                info=f"psycopg2.Error. Error message: {e}",
            )

    def _update_project(
        self,
        project_dict: json,
        namespace: str,
        proj_name: str,
        tag: str,
        project_digest: str,
        number_of_samples: int,
    ) -> UploadResponse:
        """
        Update existing project by providing all necessary information.
        :param project_dict: project dictionary in json format
        :param namespace: project namespace
        :param proj_name: project name
        :param tag: project tag
        :param project_digest: project digest
        :param number_of_samples: number of samples in project
        :return: NoReturn
        """

        cursor = self.con.pg_connection.cursor()

        if self.project_exists(namespace=namespace, name=proj_name, tag=tag):
            try:
                _LOGGER.info(f"Updating {proj_name} project...")
                sql = f"""UPDATE {DB_TABLE_NAME}
                    SET {DIGEST_COL} = %s, {PROJ_COL}= %s, {N_SAMPLES_COL}= %s, {LAST_UPDATE_DATE_COL} = %s
                    WHERE {NAMESPACE_COL} = %s and {NAME_COL} = %s and {TAG_COL} = %s;"""
                cursor.execute(
                    sql,
                    (
                        project_digest,
                        project_dict,
                        number_of_samples,
                        datetime.datetime.now(),
                        namespace,
                        proj_name,
                        tag,
                    ),
                )
                self.con.commit_to_database()
                _LOGGER.info(
                    f"Project '{namespace}/{proj_name}:{tag}' has been updated!"
                )
                return UploadResponse(
                    registry_path=f"{namespace}/{proj_name}:{tag}",
                    log_stage="update_project",
                    status="success",
                    info=f"Project was updated",
                )

            except psycopg2.Error as err:
                _LOGGER.error(
                    f"Error occurred while updating the project! Error: {err}"
                )
                return UploadResponse(
                    registry_path=f"{namespace}/{proj_name}:{tag}",
                    log_stage="update_project",
                    status="failure",
                    info=f"Error in executing sql! Error message: {err}",
                )

        else:
            _LOGGER.error("Project does not exist! No project will be updated!")
            return UploadResponse(
                registry_path=f"{namespace}/{proj_name}:{tag}",
                log_stage="update_project",
                status="failure",
                info="project does not exist!",
            )

    def update_item(
        self,
        update_dict: Union[dict, UpdateItems],
        namespace: str,
        name: str,
        tag: str,
    ) -> UploadResponse:
        """
        Update partial parts of the project record
        :param update_dict: dict with update key->values. Dict structure:
            {
                    project: Optional[peppy.Project]
                    is_private: Optional[bool]
                    tag: Optional[str]
                    name: Optional[str]
            }
            *project_value should contain name and description
        :param namespace: project namespace
        :param name: project name
        :param tag: project tag
        :return: ResponseModel with information if project was updated
        """
        cursor = self.con.pg_connection.cursor()

        if isinstance(update_dict, UpdateItems):
            update_values = update_dict
        else:
            update_values = UpdateItems(**update_dict)

        if self.project_exists(namespace=namespace, name=name, tag=tag):
            try:
                update_final = UpdateModel()

                if update_values.project_value is not None:
                    update_final = UpdateModel(
                        project_value=update_values.project_value.to_dict(
                            extended=True
                        ),
                        name=update_values.project_value.name,
                        digest=create_digest(
                            update_values.project_value.to_dict(extended=True)
                        ),
                        last_update_date=datetime.datetime.now(),
                        number_of_samples=len(update_values.project_value.samples),
                    )

                if update_values.tag is not None:
                    update_final = UpdateModel(
                        tag=update_values.tag, **update_final.dict(exclude_unset=True)
                    )

                if update_values.is_private is not None:
                    update_final = UpdateModel(
                        is_private=update_values.is_private,
                        **update_final.dict(exclude_unset=True),
                    )

                if update_values.name is not None:
                    update_final = UpdateModel(
                        name=update_values.name, **update_final.dict(exclude_unset=True)
                    )

                set_sql, set_values = self.__create_update_set(update_final)
                sql = f"""UPDATE {DB_TABLE_NAME}
                    {set_sql}
                    WHERE {NAMESPACE_COL} = %s and {NAME_COL} = %s and {TAG_COL} = %s;"""
                _LOGGER.debug("Updating items...")
                cursor.execute(
                    sql,
                    (*set_values, namespace, name, tag),
                )
                _LOGGER.info(
                    f"Record '{namespace}/{name}:{tag}' was successfully updated!"
                )
                self.con.commit_to_database()

            except Exception as err:
                _LOGGER.error(f"Error while updating project! Error: {err}")
                return UploadResponse(
                    registry_path=f"{namespace}/{name}:{tag}",
                    log_stage="update_item",
                    status="failure",
                    info=f"Error in executing SQL. {err}!",
                )
        else:
            _LOGGER.error("Project does not exist! No project will be updated!")
            return UploadResponse(
                registry_path=f"{namespace}/{name}:{tag}",
                log_stage="update_item",
                status="failure",
                info="Project does not exist!",
            )

        return UploadResponse(
            registry_path=f"{namespace}/{name}:{tag}",
            log_stage="update_item",
            status="success",
            info="Record was successfully updated!",
        )

    @staticmethod
    def __create_update_set(update_info: UpdateModel) -> Tuple[str, tuple]:
        """
        Create sql SET string by passing UpdateModel that later is converted to dict
        :param update_info: UpdateModel (similar to database model)
        :return: {sql_string (contains db keys) and updating values}
        """
        _LOGGER.debug("Creating SET SQL string to update project")
        sql_string = f"""SET """
        sql_values = []

        first = True
        for key, val in update_info.dict(exclude_none=True).items():
            if first:
                sql_string = "".join([sql_string, f"{key} = %s"])
                first = False
            else:
                sql_string = ", ".join([sql_string, f"{key} = %s"])

            if isinstance(val, dict):
                input_val = json.dumps(val)
            else:
                input_val = val

            sql_values.append(input_val)

        return sql_string, tuple(sql_values)

    def project_exists(
        self,
        namespace: str = None,
        name: str = None,
        tag: str = None,
    ) -> bool:
        """
        Checking if project exists in the database
        :param namespace: project namespace
        :param name: project name
        :param tag: project tag
        :return: Returning True if project exist
        """
        if namespace is None:
            namespace = DEFAULT_NAMESPACE

        if tag is None:
            tag = DEFAULT_TAG

        if name is None:
            _LOGGER.error(f"Name is not specified")
            return False

        sql = f"""SELECT {ID_COL} from {DB_TABLE_NAME} 
                    WHERE {NAMESPACE_COL} = %s AND
                          {NAME_COL} = %s AND 
                          {TAG_COL} = %s;"""

        if self.con.run_sql_fetchone(sql, namespace, name, tag):
            return True
        else:
            return False
