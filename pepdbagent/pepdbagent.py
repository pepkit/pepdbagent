import datetime
import json
from hashlib import md5
from typing import List, NoReturn, Union
from urllib.parse import urlparse

import coloredlogs
import logmuse
import peppy
import psycopg2
import ubiquerg
from psycopg2.errors import NotNullViolation, UniqueViolation
from pydantic import ValidationError

from pepdbagent.models import NamespaceModel, NamespacesResponseModel, ProjectModel

from .const import *
from .exceptions import SchemaError
from .pepannot import Annotation

_LOGGER = logmuse.init_logger("pepDB_connector")
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] %(message)s",
)


class Connection:
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

        self._check_conn_db()
        _LOGGER.info(f"Connected successfully!")

    def _commit_to_database(self) -> None:
        """
        Commit to database
        """
        self.pg_connection.commit()

    def close_connection(self) -> None:
        """
        Close connection with database
        """
        self.pg_connection.close()

    def upload_project(
        self,
        project: peppy.Project,
        namespace: str = None,
        name: str = None,
        tag: str = None,
        status: str = None,
        description: str = None,
        is_private: bool = False,
        overwrite: bool = False,
    ) -> Union[NoReturn, str]:
        """
        Upload project to the database.
        Project with the key, that already exists won't be uploaded(but case, when argument
        update is set True)
        :param peppy.Project project: Project object that has to be uploaded to the DB
        :param namespace: namespace of the project (Default: 'other')
        :param name: name of the project (Default: name is taken from the project object)
        :param tag: tag (or version) of the project
        :param status: status of the project
        :param description: description of the project
        :param is_private: boolean value if the project should be visible just for user that creates it
        :param overwrite: if project exists overwrite the project, otherwise upload it.
            [Default: False - project won't be overwritten if it exists in db]
        """
        cursor = self.pg_connection.cursor()
        try:
            if namespace is None:
                namespace = DEFAULT_NAMESPACE
            if tag is None:
                tag = DEFAULT_TAG

            proj_dict = project.to_dict(extended=True)

            proj_digest = self._create_digest(proj_dict)

            if name:
                proj_name = name
            else:
                proj_name = proj_dict["name"]

            # creating annotation:
            proj_annot = Annotation(
                status=status,
                description=description,
                last_update=str(datetime.datetime.now()),
                n_samples=len(project.samples),
                is_private=is_private,
            )

            proj_dict = json.dumps(proj_dict)

            try:
                _LOGGER.info(f"Uploading {proj_name} project...")

                sql_base = f"""INSERT INTO {DB_TABLE_NAME}({NAMESPACE_COL}, {NAME_COL}, {TAG_COL}, {DIGEST_COL}, {PROJ_COL}, {ANNO_COL})
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING {ID_COL};"""

                cursor.execute(
                    sql_base,
                    (
                        namespace,
                        proj_name,
                        tag,
                        proj_digest,
                        proj_dict,
                        proj_annot.json(),
                    ),
                )
                proj_id = cursor.fetchone()[0]
                _LOGGER.info(
                    f"Project: '{namespace}/{proj_name}:{tag}' was successfully uploaded."
                )

                self._commit_to_database()
                cursor.close()

            except UniqueViolation:
                if overwrite:

                    self._update_project(
                        project_dict=proj_dict,
                        namespace=namespace,
                        proj_name=proj_name,
                        tag=tag,
                        project_digest=proj_digest,
                        proj_annot=proj_annot,
                    )
                else:
                    _LOGGER.warning(
                        f"Namespace, name and tag already exists. Project won't be uploaded. "
                        f"Solution: Set overwrite value as True (project will be overwritten),"
                        f" or change tag!"
                    )

            except NotNullViolation:
                _LOGGER.error(
                    f"Name of the project wasn't provided. Project will not be uploaded"
                )
                return f"Error_name: {namespace}/{proj_name}:{tag}"

        except psycopg2.Error as e:
            _LOGGER.error(
                f"Error while uploading project. Project hasn't been uploaded!"
            )
            cursor.close()
            return f"Error_psycopg2: {namespace}/{name}:{tag}"

    def _update_project(
        self,
        project_dict: json,
        namespace: str,
        proj_name: str,
        tag: str,
        project_digest: str,
        proj_annot,
    ) -> NoReturn:
        """
        Update existing project by providing all necessary information.
        :param project_dict: project dictionary in json format
        :param namespace: project namespace
        :param proj_name: project name
        :param tag: project tag
        :param project_digest: project digest
        :param proj_annot: project annotation in Annotation object
        :return: NoReturn
        """

        cursor = self.pg_connection.cursor()

        if self.project_exists(namespace=namespace, name=proj_name, tag=tag):
            try:
                _LOGGER.info(f"Updating {proj_name} project...")
                sql = f"""UPDATE {DB_TABLE_NAME}
                    SET {DIGEST_COL} = %s, {PROJ_COL}= %s, {ANNO_COL}= %s
                    WHERE {NAMESPACE_COL} = %s and {NAME_COL} = %s and {TAG_COL} = %s;"""
                cursor.execute(
                    sql,
                    (
                        project_digest,
                        project_dict,
                        proj_annot.json(),
                        namespace,
                        proj_name,
                        tag,
                    ),
                )
                self._commit_to_database()
                _LOGGER.info(
                    f"Project '{namespace}/{proj_name}:{tag}' has been updated!"
                )
            except psycopg2.Error:
                _LOGGER.error("Error occurred while updating the project!")

        else:
            _LOGGER.error("Project does not exist! No project will be updated!")

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
        Retrieving project from database by specifying project registry_path, name, or digest
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
                select {ID_COL}, {PROJ_COL}, {ANNO_COL} from {DB_TABLE_NAME}
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
            is_private = found_prj[2].get("is_private") or False
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
                f"No project found for supplied input. Did you supply a valid namespace and project? {sql_q}"
            )
            return None

    def get_projects_in_namespace(
        self,
        user: str,
        namespace: str = None,
        tag: str = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[peppy.Project]:
        """
        Get a list of projects in provided namespace.
        Default limit is 100, to change it use limit and offset parameter
        :param namespace: The namespace to fetch all projects from.
        :param tag: The tag to fetch all projects from.
        :param limit: The maximum number of items to return.
        :param offset: The index of the first item to return. Default: 0 (the first item).
            Use with limit to get the next set of items.
        :return: a list of peppy.Project instances for the requested projects.
        """
        offset_number = limit * offset
        if namespace:
            if tag:
                sql_q = (
                    f"select {ID_COL}, {PROJ_COL}, {ANNO_COL} "
                    f"from {DB_TABLE_NAME} "
                    f"where namespace = %s and tag = %s "
                    f"limit {limit} offset {offset_number}"
                )
                results = self._run_sql_fetchall(sql_q, namespace, tag)
            else:
                sql_q = (
                    f"select {ID_COL}, {PROJ_COL}, {ANNO_COL} from {DB_TABLE_NAME} where namespace = %s"
                    f" limit {limit} offset {offset_number}"
                )
                results = self._run_sql_fetchall(sql_q, namespace)

        # if only tag is provided
        elif tag:
            sql_q = (
                f"select {ID_COL}, {PROJ_COL}, {ANNO_COL} from {DB_TABLE_NAME} where tag = %s"
                f" limit {limit} offset {offset_number}"
            )
            results = self._run_sql_fetchall(sql_q, tag)

        else:
            _LOGGER.warning(f"Incorrect input!")
            results = []

        # extract out the project config dictionary from the query
        result_list = []
        for project in results:
            try:
                project_object = peppy.Project().from_dict(project[1])
                project_object.is_private = project[2].get("is_private")
                if not project_object.is_private or (
                    project_object.is_private and namespace == user
                ):
                    result_list.append(project_object)
            except Exception:
                _LOGGER.error(
                    f"Error in init project. Error occurred in peppy. Project id={project[0]}"
                )

        return result_list

    def get_namespace_info(
        self, namespace: str, user: str, user_organizations: List[str]
    ):
        """
        Fetch projects information from a particular namespace. This doesn't retrieve full project
        objects.

        :param namespace: the namespace to fetch
        :return: A dictionary representation of the namespace in the database.
        Return dictionary schema:
            namespace,
            n_samples,
            n_projects,
            projects:(id, name, tag, digest, description, n_samples)
        """
        try:
            sql_q = f"select {ID_COL}, {NAME_COL}, {TAG_COL}, {DIGEST_COL}, {ANNO_COL} from {DB_TABLE_NAME} where {NAMESPACE_COL} = %s"
            results = self._run_sql_fetchall(sql_q, namespace)

            projects = []
            for project_data in results:
                annotation = Annotation(**project_data[4])
                projects.append(
                    ProjectModel(
                        id=project_data[0],
                        name=project_data[1],
                        tag=project_data[2],
                        digest=project_data[3],
                        description=annotation.description,
                        number_of_samples=annotation.number_of_samples,
                        is_private=annotation.is_private,
                    )
                )
            namespace = NamespaceModel(
                namespace=namespace,
                projects=projects,
                number_of_samples=sum(map(lambda p: p.number_of_samples, projects)),
                number_of_projects=len(projects),
            )
            return self._get_projects_from_namespace_that_user_is_authorized_for(
                namespace, user, user_organizations
            )

        except (TypeError, ValidationError):
            _LOGGER.warning(
                f"Error occurred while getting data from '{namespace}' namespace"
            )

    @staticmethod
    def _get_projects_from_namespace_that_user_is_authorized_for(
        namespace: NamespaceModel,
        user: str,
        user_organizations: List[str],
    ):
        """
        Iterate over projects within namespace and return the ones, that given user is authorized to view.
        Usually the projects are public projects + projects within user namespace.
        """
        if namespace.namespace == user or namespace.namespace in user_organizations:
            return namespace
        else:
            projects_that_user_is_authorized_for = []
            for project in namespace.projects:
                if not project.is_private:
                    projects_that_user_is_authorized_for.append(project)

            namespace.projects = projects_that_user_is_authorized_for
            return namespace

    def get_namespaces_info_by_list(
        self, user: str, user_organizations: List[str]
    ) -> list:
        """
        Get list of all available namespaces.
        """

        sql_q = f"""SELECT DISTINCT {NAMESPACE_COL} FROM {DB_TABLE_NAME};"""
        if query_result := self._run_sql_fetchall(sql_q):
            namespaces = [namespace[0] for namespace in query_result]
        else:
            namespaces = []

        namespaces_with_info = NamespacesResponseModel(
            **{
                "namespaces": self.get_namespace_info_from_list(
                    namespaces, user, user_organizations
                )
            }
        )
        return self._filter_namespaces_for_privacy(namespaces_with_info)

    def get_namespace_info_from_list(
        self, namespaces: List, user: str, organizations: List[str]
    ) -> List:
        """
        Wrapper that transforms list of namespaces to list of namespaces info.
        """
        namespaces_list = []
        for namespace in namespaces:
            try:
                namespaces_list.append(
                    self.get_namespace_info(namespace, user, organizations)
                )
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
                    {ANNO_COL}
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

        annot = Annotation(
            registry=f"{found_prj[0]}/{found_prj[1]}:{found_prj[2]}",
            annotation_dict=found_prj[3],
        )

        return annot

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

    def get_namespace_annotation(self, namespace: str = None) -> dict:
        """
        Retrieving namespace annotation dict.
        Data that will be retrieved: number of tags, projects and samples
        If namespace is None it will retrieve dict with all namespace annotations.
        :param namespace: project namespace
        """
        sql_q = f"""
        select {NAMESPACE_COL}, count(DISTINCT {TAG_COL}) as n_tags , 
        count({NAME_COL}) as 
        n_namespace, SUM(({ANNO_COL} ->> 'n_samples')::int) 
        as n_samples 
            from {DB_TABLE_NAME}
                group by {NAMESPACE_COL};
        """
        result = self._run_sql_fetchall(sql_q)
        anno_dict = {}

        for name_sp_result in result:
            anno_dict[name_sp_result[0]] = {
                "namespace": name_sp_result[0],
                "n_tags": name_sp_result[1],
                "n_projects": name_sp_result[2],
                "n_samples": name_sp_result[3],
            }

        if namespace:
            try:
                return anno_dict[namespace]
            except KeyError:
                _LOGGER.warning(f"Namespace '{namespace}' was not found.")
                return {
                    "namespace": namespace,
                    "n_tags": 0,
                    "n_projects": 0,
                    "n_samples": 0,
                }

        return anno_dict

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

    def project_exists_by_registry_path(
        self,
        registry_path: str,
    ) -> bool:
        """
        Checking if project exists in the database
        :param registry_path: project registry path
        :return: Returning True if project exist
        """

        reg = ubiquerg.parse_registry_path(
            registry_path,
            defaults=[
                ("namespace", DEFAULT_NAMESPACE),
                ("item", None),
                ("tag", DEFAULT_TAG),
            ],
        )

        if self.project_exists(
            namespace=reg["namespace"], name=reg["item"], tag=reg["tag"]
        ):
            return True
        else:
            return False

    def _run_sql_fetchone(self, sql_query: str, *argv) -> list:
        """
        Fetching one result by providing sql query and arguments
        :param sql_query: sql string that has to run
        :param argv: arguments that has to be added to sql query
        :return: set of query result
        """
        cursor = self.pg_connection.cursor()
        try:
            cursor.execute(sql_query, argv)
            output_result = cursor.fetchone()

            # must run check here since None is not iterable.
            if output_result is not None:
                return list(output_result)
            else:
                return []
        except psycopg2.Error as e:
            _LOGGER.error(f"Error occurred while running query: {e}")
        finally:
            cursor.close()

    def _run_sql_fetchall(self, sql_query: str, *argv) -> list:
        """
        Fetching all result by providing sql query and arguments
        :param str sql_query: sql string that has to run
        :param argv: arguments that has to be added to sql query
        :return: set of query result
        """
        cursor = self.pg_connection.cursor()
        try:
            cursor.execute(sql_query, argv)
            output_result = cursor.fetchall()
            cursor.close()
            return output_result
        except psycopg2.Error as e:
            _LOGGER.error(f"Error occurred while running query: {e}")
        finally:
            cursor.close()

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
                project_dict[SAMPLE_RAW_DICT_KEY],
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

    def __exit__(self):
        self.close_connection()

    def __str__(self):
        return f"Connection to the database: '{self.db_name}' is set!"
