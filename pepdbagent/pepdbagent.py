from typing import List, Union, NoReturn
import psycopg2
from psycopg2.errors import UniqueViolation, NotNullViolation
import json
import logmuse
import peppy
from hashlib import md5
from itertools import chain
import ubiquerg
import datetime

from .utils import all_elements_are_strings, is_valid_resgistry_path
from .const import *
from .exceptions import SchemaError
from .pepannot import Annotation

import coloredlogs
from urllib.parse import urlparse

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

    def _commit_connection(self) -> None:
        """
        Commit connection
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
        anno: dict = None,
        update: bool = False,
        is_private: bool = False,
    ) -> NoReturn:
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
        :param anno: dict with annotations about current project
        :param update: boolean value if existed project has to be updated (if project with the same
        registry path already exists)
        :param is_private: boolean value if the project should be visible just for user that creates it
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
            proj_annot = Annotation().create_new_annotation(
                status=status,
                description=description,
                last_update=str(datetime.datetime.now()),
                n_samples=len(project.samples),
                anno_dict=anno,
                is_private=is_private,
            )

            proj_dict = json.dumps(proj_dict)

            try:
                _LOGGER.info(f"Uploading {proj_name} project...")
                sql = f"""INSERT INTO {DB_TABLE_NAME}({NAMESPACE_COL}, {NAME_COL}, {TAG_COL}, {DIGEST_COL}, {PROJ_COL}, {ANNO_COL})
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING {ID_COL};"""
                cursor.execute(
                    sql,
                    (
                        namespace,
                        proj_name,
                        tag,
                        proj_digest,
                        proj_dict,
                        proj_annot.get_json(),
                    ),
                )
                proj_id = cursor.fetchone()[0]
                _LOGGER.info(
                    f"Project: '{namespace}/{proj_name}:{tag}' was successfully uploaded."
                )

                self._commit_connection()
                cursor.close()

            except UniqueViolation:
                if update:
                    self.update_project(
                        namespace=namespace,
                        name=proj_name,
                        tag=tag,
                        project=project,
                        anno=dict(proj_annot),
                    )
                else:
                    _LOGGER.warning(
                        f"Namespace, name and tag already exists. Project won't be uploaded. "
                        f"Solution: Set update value as True (project will be overwritten),"
                        f" or change tag!"
                    )
            except NotNullViolation:
                _LOGGER.error(
                    f"Name of the project wasn't provided. Project will not be uploaded"
                )

        except psycopg2.Error as e:
            _LOGGER.error(
                f"Error while uploading project. Project hasn't been uploaded!"
            )
            cursor.close()

    def update_project(
        self,
        project: peppy.Project,
        namespace: str = None,
        name: str = None,
        tag: str = None,
        status: str = None,
        description: str = None,
        anno: dict = None,
    ) -> None:
        """
        Upload project to the database
        :param peppy.Project project: Project object that has to be uploaded to the DB
        :param namespace: namespace of the project (Default: 'other')
        :param name: name of the project (Default: name is taken from the project object)
        :param tag: tag (or version) of the project
        :param status: status of the project
        :param description: description of the project
        :param anno: dict with annotations about current project
        """

        cursor = self.pg_connection.cursor()

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
        proj_annot = Annotation().create_new_annotation(
            status=status,
            description=description,
            last_update=str(datetime.datetime.now()),
            n_samples=len(project.samples),
            anno_dict=anno,
        )

        proj_dict = json.dumps(proj_dict)

        if self.project_exists(namespace=namespace, name=proj_name, tag=tag):
            try:
                _LOGGER.info(f"Updating {proj_name} project...")
                sql = f"""UPDATE {DB_TABLE_NAME}
                    SET {DIGEST_COL} = %s, {PROJ_COL}= %s, {ANNO_COL}= %s
                    WHERE {NAMESPACE_COL} = %s and {NAME_COL} = %s and {TAG_COL} = %s;"""
                cursor.execute(
                    sql,
                    (
                        proj_digest,
                        proj_dict,
                        proj_annot.get_json(),
                        namespace,
                        proj_name,
                        tag,
                    ),
                )
                _LOGGER.info(
                    f"Project '{namespace}/{proj_name}:{tag}' has been updated!"
                )
            except psycopg2.Error:
                _LOGGER.error("Error occurred while updating the project!")
        else:
            _LOGGER.error("Project does not exist! No project will be updated!")

    def get_project_by_registry(
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
                    f"select {ID_COL}, {PROJ_COL} "
                    f"from {DB_TABLE_NAME} "
                    f"where namespace = %s and tag = %s "
                    f"limit {limit} offset {offset_number}"
                )
                results = self._run_sql_fetchall(sql_q, namespace, tag)
            else:
                sql_q = (
                    f"select {ID_COL}, {PROJ_COL} from {DB_TABLE_NAME} where namespace = %s"
                    f" limit {limit} offset {offset_number}"
                )
                results = self._run_sql_fetchall(sql_q, namespace)

        # if only tag is provided
        elif tag:
            sql_q = (
                f"select {ID_COL}, {PROJ_COL} from {DB_TABLE_NAME} where tag = %s"
                f" limit {limit} offset {offset_number}"
            )
            results = self._run_sql_fetchall(sql_q, tag)
            print(results)

        else:
            _LOGGER.warning(f"Incorrect input!")
            results = []

        # extract out the project config dictionary from the query
        result_list = []
        for p in results:
            try:
                result_list.append(peppy.Project().from_dict(p[1]))
            except Exception:
                _LOGGER.error(
                    f"Error in init project. Error occurred in peppy. Project id={p[0]}"
                )

        return result_list

    def get_namespace_info(self, namespace: str) -> dict:
        """
        Fetch a particular namespace from the database. This doesn't retrieve full project
        objects. For that, one should utilize the `get_projects(namespace=...)` function.

        :param namespace: the namespace to fetch
        :return: A dictionary representation of the namespace in the database
        """
        try:
            sql_q = f"select {ID_COL}, {NAME_COL}, {TAG_COL}, {DIGEST_COL}, {ANNO_COL} from {DB_TABLE_NAME} where {NAMESPACE_COL} = %s"
            results = self._run_sql_fetchall(sql_q, namespace)
            projects = [
                {
                    "id": p[0],
                    "name": p[1],
                    "tag": p[2],
                    "digest": p[3],
                    "description": Annotation(p[4]).description,
                    "n_samples": Annotation(p[4]).n_samples,
                }
                for p in results
            ]
            result = {
                "namespace": namespace,
                "projects": projects,
                "n_samples": sum(map(lambda p: p["n_samples"], projects)),
                "n_projects": len(projects),
            }
            return result
        except TypeError:
            _LOGGER.warning(
                f"Error occurred while getting data from '{namespace}' namespace"
            )

    def get_namespaces_info_by_list(
        self, namespaces: List[str] = None, names_only: bool = False
    ) -> list:
        """
        Get list of all available namespaces.

        :param List[str] namespaces: An optional list of namespaces to fetch.
        :param bool names_only: Flag to indicate you only want unique namespace names
        :return: list of available namespaces
        """
        if namespaces is not None:
            # coerce to list if not
            if isinstance(namespaces, str):
                namespaces = [namespaces]
            # verify all strings
            elif not all_elements_are_strings(namespaces):
                raise ValueError(
                    f"Namespace list must only contain str. Supplied: {namespaces}"
                )
        else:
            sql_q = f"""SELECT DISTINCT {NAMESPACE_COL} FROM {DB_TABLE_NAME};"""
            namespaces = [n[0] for n in self._run_sql_fetchall(sql_q)]
            if names_only:
                return [n for n in namespaces]

        namespaces_list = []
        for ns in namespaces:
            try:
                namespaces_list.append(self.get_namespace_info(ns))
            except TypeError:
                _LOGGER.warning(
                    f"Warning: Error in collecting projects from database. {ns} wasn't collected!"
                )

        return namespaces_list

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

    def get_project_annotation_by_registry(
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

    def project_exists_by_registry(
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
        namespace = reg["namespace"]
        name = reg["item"]
        tag = reg["tag"]

        if self.project_exists(namespace=namespace, name=name, tag=tag):
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
            json.dumps(project_dict[SAMPLE_RAW_DICT_KEY], sort_keys=True).encode(
                "utf-8"
            )
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
