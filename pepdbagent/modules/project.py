import datetime
import json
from typing import Union, Tuple
import logging
import peppy
from psycopg2.errors import NotNullViolation, UniqueViolation

from pepdbagent.models import (
    UpdateModel,
    UpdateItems,
)
from pepdbagent.base_connection import BaseConnection
from pepdbagent.const import *
from pepdbagent.utils import create_digest, registry_path_converter
from pepdbagent.exceptions import (
    ProjectNotFoundError,
    ProjectUniqueNameError,
)

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
        tag: str = None,
        raw: bool = False,
    ) -> Union[peppy.Project, dict, None]:
        """
        Retrieve project from database by specifying namespace, name and tag
        :param namespace: namespace of the project
        :param name: name of the project (Default: name is taken from the project object)
        :param tag: tag (or version) of the project.
        :param raw: retrieve unprocessed (raw) PEP dict.
        :return: peppy.Project object with found project or dict with unprocessed
            PEP elements: {
                name: str
                description: str
                _config: dict
                _sample_dict: dict
                _subsample_dict: dict
            }
        """
        if tag is None:
            tag = DEFAULT_TAG

        sql_q = f"""
                select {ID_COL}, {PROJ_COL}, {PRIVATE_COL} from {DB_TABLE_NAME}
                """

        sql_q = (
            f""" {sql_q} where {NAME_COL}=%s and {NAMESPACE_COL}=%s and {TAG_COL}=%s;"""
        )
        found_prj = self.con.run_sql_fetchone(sql_q, name, namespace, tag)

        if found_prj:
            _LOGGER.info(f"Project has been found: {found_prj[0]}")
            project_value = found_prj[1]
            is_private = found_prj[2]
            if raw:
                return project_value
            else:
                project_obj = peppy.Project().from_dict(project_value)
                project_obj.is_private = is_private
                return project_obj

        else:
            raise ProjectNotFoundError(
                f"No project found for supplied input: '{namespace}/{name}:{tag}'. "
                f"Did you supply a valid namespace and project?"
            )

    def get_by_rp(
        self,
        registry_path: str,
        raw: bool = False,
    ) -> Union[peppy.Project, dict, None]:
        """
        Retrieve project from database by specifying project registry_path
        :param registry_path: project registry_path [e.g. namespace/name:tag]
        :param raw: retrieve unprocessed (raw) PEP dict.
        :return: peppy.Project object with found project or dict with unprocessed
            PEP elements: {
                name: str
                description: str
                _config: dict
                _sample_dict: dict
                _subsample_dict: dict
            }
        """
        namespace, name, tag = registry_path_converter(registry_path)
        return self.get(namespace=namespace, name=name, tag=tag, raw=raw)

    def delete(
        self,
        namespace: str = None,
        name: str = None,
        tag: str = None,
    ) -> None:
        """
        Delete record from database
        :param namespace: Namespace
        :param name: Name
        :param tag: Tag
        :return: None
        """
        cursor = self.con.pg_connection.cursor()
        sql_delete = f"""DELETE FROM {DB_TABLE_NAME} 
            WHERE {NAMESPACE_COL} = %s and {NAME_COL} = %s and {TAG_COL} = %s;"""

        if not self.exists(namespace=namespace, name=name, tag=tag):
            raise ProjectNotFoundError(
                f"Can't delete unexciting project: '{namespace}/{name}:{tag}'."
            )

        try:
            cursor.execute(sql_delete, (namespace, name, tag))
            _LOGGER.info(f"Project '{namespace}/{name}:{tag} was successfully deleted'")
        except Exception as err:
            _LOGGER.error(f"Error while deleting project. Message: {err}")
        finally:
            cursor.close()
            return None

    def delete_by_rp(
        self,
        registry_path: str,
    ) -> None:
        """
        Delete record from database by using registry_path
        :param registry_path: Registry path of the project ('namespace/name:tag')
        :return: None
        """
        namespace, name, tag = registry_path_converter(registry_path)
        return self.delete(namespace=namespace, name=name, tag=tag)

    def create(
        self,
        project: peppy.Project,
        namespace: str,
        name: str = None,
        tag: str = None,
        is_private: bool = False,
        overwrite: bool = False,
        update_only: bool = False,
    ) -> None:
        """
        Upload project to the database.
        Project with the key, that already exists won't be uploaded(but case, when argument
        update is set True)
        :param peppy.Project project: Project object that has to be uploaded to the DB
        :param namespace: namespace of the project (Default: 'other')
        :param name: name of the project (Default: name is taken from the project object)
        :param tag: tag (or version) of the project.
        :param is_private: boolean value if the project should be visible just for user that creates it.
        :param overwrite: if project exists overwrite the project, otherwise upload it.
            [Default: False - project won't be overwritten if it exists in db]
        :param update_only: if project exists overwrite it, otherwise do nothing.  [Default: False]
        :return: None
        """
        cursor = self.con.pg_connection.cursor()
        if tag is None:
            tag = DEFAULT_TAG

        proj_dict = project.to_dict(extended=True)

        if name:
            proj_name = name
        else:
            proj_name = proj_dict["name"]

        proj_dict["name"] = name

        proj_digest = create_digest(proj_dict)

        number_of_samples = len(project.samples)
        proj_dict = json.dumps(proj_dict)

        if update_only:
            _LOGGER.info(
                f"Update_only argument is set True. Updating project {proj_name} ..."
            )
            self._overwrite(
                project_dict=proj_dict,
                namespace=namespace,
                proj_name=proj_name,
                tag=tag,
                project_digest=proj_digest,
                number_of_samples=number_of_samples,
            )
            return None
        else:
            try:
                _LOGGER.info(f"Uploading {namespace}/{proj_name}:{tag} project...")

                sql_base = f"""INSERT INTO {DB_TABLE_NAME} 
                ({NAMESPACE_COL}, {NAME_COL}, {TAG_COL}, {DIGEST_COL}, {PROJ_COL}, {N_SAMPLES_COL}, 
                    {PRIVATE_COL}, {SUBMISSION_DATE_COL}, {LAST_UPDATE_DATE_COL})
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
                return None

            except UniqueViolation:
                if overwrite:
                    self._overwrite(
                        project_dict=proj_dict,
                        namespace=namespace,
                        proj_name=proj_name,
                        tag=tag,
                        project_digest=proj_digest,
                        number_of_samples=number_of_samples,
                    )
                    return None
                else:
                    raise ProjectUniqueNameError(
                        f"Namespace, name and tag already exists. Project won't be "
                        f"uploaded. Solution: Set overwrite value as True"
                        f" (project will be overwritten), or change tag!"
                    )

            except NotNullViolation as err:
                raise ValueError(
                    f"Name of the project wasn't provided. Project will not be uploaded. Error: {err}"
                )

    def _overwrite(
        self,
        project_dict: json,
        namespace: str,
        proj_name: str,
        tag: str,
        project_digest: str,
        number_of_samples: int,
    ) -> None:
        """
        Update existing project by providing all necessary information.
        :param project_dict: project dictionary in json format
        :param namespace: project namespace
        :param proj_name: project name
        :param tag: project tag
        :param project_digest: project digest
        :param number_of_samples: number of samples in project
        :return: None
        """

        cursor = self.con.pg_connection.cursor()

        if self.exists(namespace=namespace, name=proj_name, tag=tag):
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
                f"Project '{namespace}/{proj_name}:{tag}' has been updated successfully!"
            )
            return None

        else:
            raise ProjectNotFoundError(
                "Project does not exist! No project will be updated!"
            )

    def update(
        self,
        update_dict: Union[dict, UpdateItems],
        namespace: str,
        name: str,
        tag: str,
    ) -> None:
        """
        Update partial parts of the record in db
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
        :return: None
        """
        cursor = self.con.pg_connection.cursor()

        if isinstance(update_dict, UpdateItems):
            update_values = update_dict
        else:
            update_values = UpdateItems(**update_dict)

        if self.exists(namespace=namespace, name=name, tag=tag):
            update_final = UpdateModel()

            if update_values.project_value is not None:
                update_final = UpdateModel(
                    project_value=update_values.project_value.to_dict(extended=True),
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
            _LOGGER.info(f"Record '{namespace}/{name}:{tag}' was successfully updated!")
            self.con.commit_to_database()

        else:
            raise ProjectNotFoundError("No items will be updated!")

        return None

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

    def exists(
        self,
        namespace: str = None,
        name: str = None,
        tag: str = None,
    ) -> bool:
        """
        Check if project exists in the database.
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
