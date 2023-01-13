import datetime
import json
from hashlib import md5
from typing import List, NoReturn, Union, Tuple
from urllib.parse import urlparse

import coloredlogs
import logging
import peppy
import psycopg2
import ubiquerg
from psycopg2.errors import NotNullViolation, UniqueViolation
from pydantic import ValidationError

from .models import (
    NamespaceModel,
    NamespacesResponseModel,
    ProjectModel,
    UploadResponse,
    UpdateModel,
    UpdateItems,
    Annotation,
)
from .search import Search
from .base import BaseConnection
from .const import *
from .exceptions import SchemaError

_LOGGER = logging.getLogger("pepdbagent")
_PEPPY_LOGGER = logging.getLogger("peppy")
coloredlogs.install(
    logger=_PEPPY_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] [PEPPY] %(message)s",
)


class Connection(BaseConnection):
    """
    A class to connect to pep-db and upload, download, read and process pep projects.
    """

    def __init__(
        self,
        host="localhost",
        port=5432,
        database="pep-db",
        user=None,
        password=None,
        dsn=None,
    ):
        """
        Initialize connection to the pep_db database. You can use The basic connection parameters
        or libpq connection string.
        :param host: database server address e.g., localhost or an IP address.
        :param port: the port number that defaults to 5432 if it is not provided.
        :param database: the name of the database that you want to connect.
        :param user: the username used to authenticate.
        :param password: password used to authenticate.
        :param dsn: libpq connection string using the dsn parameter
        (e.g. "localhost://username:password@pdp_db:5432")
        """

        _LOGGER.info(f"Initializing connection to {database}...")

        if dsn is not None:
            self.pg_connection = psycopg2.connect(dsn)
            self.db_name = urlparse(dsn).path[1:]
        else:
            self.pg_connection = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
            )
            self.db_name = database

        # Ensure data is added to the database immediately after write commands
        self.pg_connection.autocommit = True

        super().__init__(db_conn=self.pg_connection)

        self._check_conn_db()
        _LOGGER.info(f"Connected successfully!")

    def upload_project(
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
        cursor = self.pg_connection.cursor()
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

            proj_digest = self._create_digest(proj_dict)

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
                    _LOGGER.info(f"Uploading {proj_name} project...")

                    sql_base = f"""INSERT INTO {DB_TABLE_NAME}({NAMESPACE_COL}, {NAME_COL}, {TAG_COL}, {DIGEST_COL}, {PROJ_COL}, {N_SAMPLES_COL}, {PRIVATE_COL}, {SUBMISSION_DATE_COL}, {LAST_UPDATE_DATE_COL})
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

                    self._commit_to_database()
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

        cursor = self.pg_connection.cursor()

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
                self._commit_to_database()
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
        cursor = self.pg_connection.cursor()

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
                        digest=self._create_digest(
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
                self._commit_to_database()

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

            # # To update each value in the json schema separately
            # if not isinstance(val, dict):
            # else:
            #     keys_str = ""
            #     for key1, val1 in val.items():
            #         keys_str = ", ".join([keys_str, f""" '{{{str(key1)}}}', %s"""])
            #         sql_values.append(f'"{val1}"')
            #     sql_string = ', '.join([sql_string, f"""{key} = jsonb_set({key} {keys_str})"""])

        return sql_string, tuple(sql_values)

    def delete_project(
        self,
        namespace: str = None,
        name: str = None,
        tag: str = None,
    ) -> None:
        cursor = self.pg_connection.cursor()
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
        registry_path: str = None,
    ) -> NoReturn:
        if not registry_path:
            _LOGGER.error("No registry path provided! Returning empty project!")
        reg = ubiquerg.parse_registry_path(registry_path)
        namespace = reg["namespace"]
        name = reg["item"]
        tag = reg["tag"]

        return self.delete_project(namespace=namespace, name=name, tag=tag)

    def get_project_by_registry_path(
        self, registry_path: str = None
    ) -> Union[peppy.Project, None]:
        """
        Retrieving project from database by specifying project registry_path
        :param registry_path: project registry_path [e.g. namespace/name:tag]
        :return: peppy object with found project
        """
        if not registry_path:
            _LOGGER.error("No registry path provided! Returning empty project!")
            return peppy.Project()
        else:
            reg = ubiquerg.parse_registry_path(registry_path)
            namespace = reg["namespace"]
            name = reg["item"]
            tag = reg["tag"]

        return self.get_project(namespace=namespace, name=name, tag=tag)

    def get_project(
        self,
        namespace: str = None,
        name: str = None,
        tag: str = None,
    ) -> Union[peppy.Project, None]:
        """
        Retrieving project from database by specifying project name, namespace and tag
        :param namespace: project registry_path
        :param name: project name in database
        :param tag: tag of the project
        :return: peppy object with found project
        """
        if namespace is None:
            namespace = DEFAULT_NAMESPACE
        if tag is None:
            tag = DEFAULT_TAG

        sql_q = f"""
                select {ID_COL}, {PROJ_COL}, {PRIVATE_COL} from {DB_TABLE_NAME}
                """

        if name is not None:
            sql_q = f""" {sql_q} where {NAME_COL}=%s and {NAMESPACE_COL}=%s and {TAG_COL}=%s;"""
            found_prj = self._run_sql_fetchone(sql_q, name, namespace, tag)

        else:
            _LOGGER.error(
                "You haven't provided name! Execution is unsuccessful"
                "Files haven't been downloaded, returning empty project"
            )
            return None

        if found_prj:
            _LOGGER.info(f"Project has been found: {found_prj[0]}")
            project_value = found_prj[1]
            is_private = found_prj[2] or False
            try:
                project_obj = peppy.Project().from_dict(project_value)
                project_obj.is_private = is_private
                return project_obj
            except Exception:
                _LOGGER.error(
                    f"Error in init project. Error occurred in peppy. Project id={found_prj[0]}"
                )
                return None
        else:
            _LOGGER.warning(
                f"No project found for supplied input: '{namespace}/{name}:{tag}'. Did you supply a valid namespace and project?"
            )
            return None

    def get_raw_project(
        self,
        namespace: str = None,
        name: str = None,
        tag: str = None,
    ) -> Union[dict, None]:
        """
        Retrieving raw project from database by specifying project namespace, name and tag
        :param namespace: project registry_path
        :param name: project name in database
        :param tag: tag of the project
        :return: dict with raw files that are stored in dict
            return contains: {name, _config, description, _sample_dict, _subsample_dict }
            *type of _subsample_dict is null or list
        """
        if namespace is None:
            namespace = DEFAULT_NAMESPACE
        if tag is None:
            tag = DEFAULT_TAG

        if name is not None:
            sql_q = f"""
                        select {PROJ_COL} from {DB_TABLE_NAME}
                            where {NAMESPACE_COL} = %s AND {NAME_COL} = %s AND {TAG_COL}= %s;
            """
            try:
                found_prj = self._run_sql_fetchone(sql_q, namespace, name, tag)[0]
            except IndexError:
                found_prj = {}
            return found_prj

        else:
            _LOGGER.error("get_raw_project: name was not provided")
            return None

    def get_project_annotation(
        self,
        namespace: str = None,
        name: str = None,
        tag: str = None,
    ) -> Annotation:
        """
        Retrieving project annotation dict by specifying project name
        :param namespace: project registry_path - will return dict of project annotations
        :param name: project name in database
        :param tag: tag of the projects
        :return: dict of annotations
        """
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
        if namespace is None:
            namespace = DEFAULT_NAMESPACE
        if tag is None:
            tag = DEFAULT_TAG

        if name:
            sql_q = f""" {sql_q} where {NAME_COL}=%s and {NAMESPACE_COL}=%s and {TAG_COL}=%s;"""
            found_prj = self._run_sql_fetchone(sql_q, name, namespace, tag)

        else:
            _LOGGER.error(
                "You haven't provided name, digest nor id! Execution is unsuccessful"
            )
            _LOGGER.info("Files haven't been downloaded, returning empty dict")
            return Annotation()

        _LOGGER.info(f"Project has been found!")
        if len(found_prj) > 0:
            annot = Annotation(
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

        return Annotation()

    def get_project_annotation_by_registry_path(
        self,
        registry_path: str,
    ) -> Annotation:
        """
        Retrieving project annotation dict by specifying registry path
        :param registry_path: project registry_path

        :return: dict of annotations
        """

        reg = ubiquerg.parse_registry_path(registry_path)
        namespace = reg["namespace"]
        name = reg["item"]
        tag = reg["tag"]

        if namespace is None:
            namespace = DEFAULT_NAMESPACE
        if tag is None:
            tag = DEFAULT_TAG

        return self.get_project_annotation(namespace=namespace, name=name, tag=tag)

    def get_project_annotations_by_list_of_registry_paths(
        self,
        registry_paths: List[str],
    ) -> List[Annotation]:
        """
        Retrieving multiple annotation by specifying list of registry paths
        :param registry_paths: list of registry paths

        :return: list of annotation (Annotation models)
        """
        annotations_list = []
        for registry_path in registry_paths:
            reg = ubiquerg.parse_registry_path(registry_path)
            namespace = reg["namespace"]
            name = reg["item"]
            tag = reg["tag"]

            if namespace is None:
                namespace = DEFAULT_NAMESPACE
            if tag is None:
                tag = DEFAULT_TAG

            self.get_project_annotation(namespace=namespace, name=name, tag=tag)
            annotations_list.append(
                self.get_project_annotation(namespace=namespace, name=name, tag=tag)
            )
        return annotations_list

    def get_namespace_info(self, namespace: str, user: str = None):
        """
        Fetch projects information from a particular namespace. This doesn't retrieve full project
        objects.

        :param user: User or organization namespace
        :param namespace: the namespace to fetch
        :return: A dictionary representation of the namespace in the database.
        Return dictionary schema:
            namespace,
            n_samples,
            n_projects,
            projects:(id, name, tag, digest, description, n_samples)
        """
        try:
            sql_q = f"""select {ID_COL}, {NAME_COL}, {TAG_COL}, {DIGEST_COL}, {PRIVATE_COL}, {N_SAMPLES_COL}, {PROJ_COL}->>'description'
                            from {DB_TABLE_NAME} where {NAMESPACE_COL} = %s"""
            results = self._run_sql_fetchall(sql_q, namespace)

            projects = []
            for project_data in results:
                projects.append(
                    ProjectModel(
                        id=project_data[0],
                        name=project_data[1],
                        tag=project_data[2],
                        digest=project_data[3],
                        description=project_data[6],
                        number_of_samples=project_data[5],
                        is_private=project_data[4],
                    )
                )
            namespace = NamespaceModel(
                namespace=namespace,
                projects=projects,
                number_of_samples=sum(map(lambda p: p.number_of_samples, projects)),
                number_of_projects=len(projects),
            )
            return self._get_projects_from_namespace_that_user_is_authorized_for(
                namespace, user
            )

        except (TypeError, ValidationError):
            _LOGGER.warning(
                f"Error occurred while getting data from '{namespace}' namespace"
            )

    @staticmethod
    def _get_projects_from_namespace_that_user_is_authorized_for(
        namespace: NamespaceModel, user: str
    ):
        """
        Iterate over projects within namespace and return the ones, that given user is authorized to view.
        Usually the projects are public projects + projects within user namespace.
        """
        if namespace.namespace == user:
            return namespace
        else:
            projects_that_user_is_authorized_for = []
            for project in namespace.projects:
                if not project.is_private:
                    projects_that_user_is_authorized_for.append(project)

            namespace.projects = projects_that_user_is_authorized_for
            return namespace

    def get_namespaces_info_by_list(self, user: str = None) -> list:
        """
        Get list of all available namespaces.
        :param user: user (namespace) that has admin rights
        :return: list of namespaces
        """

        sql_q = f"""SELECT DISTINCT {NAMESPACE_COL} FROM {DB_TABLE_NAME};"""
        if query_result := self._run_sql_fetchall(sql_q):
            namespaces = [namespace[0] for namespace in query_result]
        else:
            namespaces = []

        namespaces_with_info = NamespacesResponseModel(
            **{"namespaces": self.get_namespace_info_from_list(namespaces, user)}
        )
        return self._filter_namespaces_for_privacy(namespaces_with_info)

    def get_namespace_info_from_list(self, namespaces: List, user: str = None) -> List:
        """
        Wrapper that transforms list of namespaces to list of namespaces info.
        """
        namespaces_list = []
        for namespace in namespaces:
            try:
                namespaces_list.append(self.get_namespace_info(namespace, user))
            except TypeError:
                _LOGGER.warning(
                    f"Warning: Error in collecting projects from database. {namespace} wasn't collected!"
                )
        return namespaces_list

    @staticmethod
    def _filter_namespaces_for_privacy(
        namespaces_with_info: NamespacesResponseModel,
    ) -> List[NamespaceModel]:
        """
        Filters the namespaces and returns only the ones matching user namespace + namespaces where at least one
        project is public.
        """
        namespaces_to_return = []

        for namespace in namespaces_with_info.namespaces:
            for project in namespace.projects:
                if not project.is_private:
                    namespaces_to_return.append(namespace)
                    break

        return namespaces_to_return

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

        if self._run_sql_fetchone(sql, namespace, name, tag):
            return True
        else:
            return False

    @staticmethod
    def _create_digest(project_dict: dict) -> str:
        """
        Create digest for PEP project
        :param project_dict: project dict
        :return: digest string
        """
        _LOGGER.info(f"Creating digest for: {project_dict['name']}")
        sample_digest = md5(
            json.dumps(
                project_dict["_sample_dict"],
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        return sample_digest

    def _check_conn_db(self) -> None:
        """
        Checking if connected database has correct column_names
        """
        a = f"""
            SELECT *
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = N'{DB_TABLE_NAME}'
            """
        result = self._run_sql_fetchall(a)
        cols_name = []
        for col in result:
            cols_name.append(col[3])
        DB_COLUMNS.sort()
        cols_name.sort()
        if DB_COLUMNS != cols_name:
            raise SchemaError

    def __str__(self):
        return f"Connection to the database: '{self.db_name}' is set!"

    def __search(self):
        return Search(self.pg_connection)

    @property
    def search(self):
        return self.__search()
