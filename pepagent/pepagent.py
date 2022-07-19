from inspect import istraceback
from typing import List, Union
import psycopg2
import json
import logmuse
import sys
import peppy
import os
from hashlib import md5
from itertools import chain
import ubiquerg
from yaml import parse

from pepagent.utils import is_valid_resgistry_path

from .exceptions import *
from .const import *
from .exceptions import SchemaError

# from pprint import pprint


_LOGGER = logmuse.init_logger("pepDB_connector")


class PepAgent:
    """
    A class to connect to pep-db and upload, download, read and process pep projects.
    """

    def __init__(
        self,
        dsn=None,
        host="localhost",
        port=5432,
        database="pep-base-sql",
        user=None,
        password=None,
    ):
        _LOGGER.info(f"Initializing connection to {database}...")

        if dsn is not None:
            self.postgresConnection = psycopg2.connect(dsn)
        else:
            self.postgresConnection = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
            )

        # Ensure data is added to the database immediately after write commands
        self.postgresConnection.autocommit = True

        self._check_conn_db()
        _LOGGER.info(f"Connected successfully!")

    def _commit_connection(self) -> None:
        """
        Commit connection
        """
        self.postgresConnection.commit()

    def close_connection(self) -> None:
        """
        Close connection with database
        """
        self.postgresConnection.close()

    def upload_project(
        self,
        project: peppy.Project,
        namespace: str = None,
        name: str = None,
        anno: dict = None,
    ) -> None:
        """
        Upload project to the database
        :param peppy.Project project: Project object that has to be uploaded to the DB
        :param str namespace: namespace of the project (Default: 'other')
        :param str name: name of the project (Default: name is taken from the project object)
        :param dict anno: dict with annotations about current project
        """
        cursor = self.postgresConnection.cursor()
        try:
            if namespace is None:
                namespace = "other"
            proj_dict = project.to_dict(extended=True)
            if name:
                proj_name = name
            else:
                proj_name = proj_dict["name"]
            proj_digest = self._create_digest(proj_dict)
            anno_info = {
                "proj_description": proj_dict["description"],
                "n_samples": len(project.samples),
            }
            if anno:
                anno_info.update(anno)
            anno_info = json.dumps(anno_info)
            proj_dict = json.dumps(proj_dict)

            sql = f"""INSERT INTO projects({NAMESPACE_COL}, {NAME_COL}, {DIGEST_COL}, {PROJ_COL}, {ANNO_COL})
            VALUES (%s, %s, %s, %s, %s) RETURNING {ID_COL};"""
            cursor.execute(
                sql,
                (
                    namespace,
                    proj_name,
                    proj_digest,
                    proj_dict,
                    anno_info,
                ),
            )

            proj_id = cursor.fetchone()[0]
            # _LOGGER.info(f"Uploading {proj_name} project!")
            print("dsfasdf")
            self._commit_connection()
            cursor.close()
            _LOGGER.info(
                f"Project: {proj_name} was successfully uploaded. The Id of this project is {proj_id}"
            )

        except psycopg2.Error as e:
            print(f"{e}")
            cursor.close()

    def get_project(
        self,
        registry: str = None,
        namespace: str = None,
        name: str = None,
        id: int = None,
        digest: str = None,
    ) -> peppy.Project:
        """
        Retrieving project from database by specifying project name or id
        :param str registry: project registry
        :param str namespace: project registry [should be used with name]
        :param str name: project name in database [should be used with namespace]
        :param str id: project id in database
        :param str digest: project digest in database
        :return: peppy object with found project
        """
        sql_q = f"""
                select {ID_COL}, {PROJ_COL} from {DB_TABLE_NAME}
                """
        if registry is not None:
            reg = ubiquerg.parse_registry_path(registry)
            namespace = reg["namespace"]
            name = reg["item"]

        if name is not None and namespace is not None:
            sql_q = f""" {sql_q} where {NAME_COL}=%s and {NAMESPACE_COL}=%s;"""
            found_prj = self.run_sql_fetchone(sql_q, (name, namespace))

        elif id is not None:
            sql_q = f""" {sql_q} where {ID_COL}=%s; """
            found_prj = self.run_sql_fetchone(sql_q, id)

        elif digest is not None:
            sql_q = f""" {sql_q} where {DIGEST_COL}=%s; """
            found_prj = self.run_sql_fetchone(sql_q, digest)

        else:
            _LOGGER.error(
                "You haven't provided neither namespace/name, digest nor id! Execution is unsuccessful"
            )
            _LOGGER.info("Files haven't been downloaded, returning empty project")
            return peppy.Project()

        if found_prj is not None:
            _LOGGER.info(f"Project has been found: {found_prj[0]}")
            project_value = found_prj[1]
            return peppy.Project(project_dict=project_value)
        else:
            _LOGGER.warn(
                f"No project found for supplied input. Did you supply a valid namespace and project? {sql_q}"
            )
            return None

    def get_projects(
        self,
        registry_paths: Union[str, List[str]] = None,
        namespace: str = None,
    ) -> List[peppy.Project]:
        """
        Get a list of projects as peppy.Project instances. This function can be used in 3 ways:
        1. Get all projects in the database (call empty)
        2. Get a list of projects using a list registry paths
        3. Get a list of projects in a namespace

        :param Union[str, List[str]] registry_paths: A list of registry paths of the form {namespace}/{project}.
        :param str namespace: The namespace to fetch all projects from.
        :return List[peppy.Project]: a list of peppy.Project instances for the requested projects.
        """
        # Case 1. Fetch all projects in database
        if all([registry_paths is None, namespace is None]):
            sql_q = f"select {NAME_COL}, {PROJ_COL} from {DB_TABLE_NAME}"
            results = self.run_sql_fetchall(sql_q)

        # Case 2. fetch list of registry paths
        elif registry_paths is not None:
            # check typing
            if all(
                [
                    not isinstance(registry_paths, str),
                    # not isinstance(registry_paths, List[str]) <-- want this, but python doesnt support type checking a subscripted generic
                    not isinstance(registry_paths, list),
                ]
            ):
                raise ValueError(
                    f"Registry paths must be of the type str or List[str]. Supplied: {type(registry_paths)}"
                )
            else:
                # coerce to list if necessary
                if isinstance(registry_paths, str):
                    registry_paths = [registry_paths]

                # check for valid registry paths
                for rpath in registry_paths:
                    if not is_valid_resgistry_path(rpath):
                        # should we raise an error or just warn with the logger?
                        raise ValueError(f"Invalid registry path supplied: '{rpath}'")

                # dynamically build filter for set of registry paths
                parametrized_filter = ""
                for i in range(len(registry_paths)):
                    parametrized_filter += "(namespace=%s and name=%s)"
                    if i < len(registry_paths) - 1:
                        parametrized_filter += " or "

            sql_q = f"select {NAME_COL}, {PROJ_COL} from {DB_TABLE_NAME} where {parametrized_filter}"
            flattened_registries = tuple(
                chain(
                    *[
                        [r["namespace"], r["item"]]
                        for r in map(
                            lambda rpath: ubiquerg.parse_registry_path(rpath),
                            registry_paths,
                        )
                    ]
                )
            )
            results = self.run_sql_fetchall(sql_q, flattened_registries)

        # Case 3. Get projects by namespace
        else:
            sql_q = f"select {NAME_COL}, {PROJ_COL} from {DB_TABLE_NAME} where namespace = %s"
            results = self.run_sql_fetchall(sql_q, (namespace,))

        # extract out the project config dictionary from the query
        return [peppy.Project(project_dict=p[1]) for p in results]

    def get_namespace(self, namespace: str) -> dict:
        """
        Fetch a particular namespace from the database. This doesnt retrieve full project
        objects. For that, one should utilize the `get_projects(namespace=...)` function.

        :param str namespace: the namespace to fetch
        :return dict: A dictionary representation of the namespace in the database
        """
        sql_q = f"select {ID_COL}, {NAME_COL}, {DIGEST_COL}, {ANNO_COL} from {DB_TABLE_NAME} where namespace = %s"
        results = self.run_sql_fetchall(sql_q, namespace)
        projects = list(
            map(
                lambda p: {
                    "id": p[0],
                    "name": p[1],
                    "digest": p[2],
                    "description": p[3]["proj_description"],
                    "n_samples": p[3]["n_samples"],
                },
                results,
            )
        )
        result = {
            "namespace": namespace,
            "projects": projects,
            "n_samples": sum(map(lambda p: p["n_samples"], projects)),
            "n_projects": len(projects),
        }
        return result

    def get_namespaces(self, namespaces: List[str] = None, names_only: bool = False) -> list:
        """
        Get list of all available namespaces

        :param List[str] namespaces: An optional list of namespaces to fetch.
        :param bool names_only: Flag to indicate you only want unique namespace names
        :return: list of available namespaces
        """
        if namespaces is not None:
            # coerce to list if not
            if isinstance(namespaces, str):
                namespaces = [namespaces]
            # verify all strings
            elif not all([
                isinstance(n, str) for n in namespaces
            ]):
                raise ValueError(f"Namespace list must only contain str. Supplied: {namespaces}")  
        else:
            sql_q = f"""SELECT DISTINCT {NAMESPACE_COL} FROM {DB_TABLE_NAME};"""
            namespaces = [n[0] for n in self.run_sql_fetchall(sql_q)]
            if names_only:
                return [n[0] for n in namespaces]
        
        return [self.get_namespace(n) for n in namespaces]

    def get_anno(
        self,
        registry: str = None,
        namespace: str = None,
        name: str = None,
        id: int = None,
        digest: str = None,
    ) -> dict:
        """
        Retrieving project annotation dict by specifying project namespace/name, id, or digest
        Additionally can return all namespace project annotations
        :param str registry: project registry
        :param str namespace: project registry - will return dict of project annotations
        :param str name: project name in database [should be used with namespace]
        :param str id: project id in database
        :param str digest: project digest in database
        :return: dict of annotations
        """
        sql_q = f"""
                select 
                    {ID_COL}, 
                    {NAMESPACE_COL},
                    {NAME_COL},
                    {ANNO_COL}
                        from {DB_TABLE_NAME}
                """
        if registry:
            reg = ubiquerg.parse_registry_path(registry)
            namespace = reg["namespace"]
            name = reg["item"]

        if not name and namespace:
            return self._get_namespace_proj_anno(namespace)

        if name and namespace:
            sql_q = f""" {sql_q} where {NAME_COL}=%s and {NAMESPACE_COL}=%s;"""
            found_prj = self.run_sql_fetchone(sql_q, (name, namespace))

        elif id:
            sql_q = f""" {sql_q} where {ID_COL}=%s; """
            found_prj = self.run_sql_fetchone(sql_q, id)

        elif digest:
            sql_q = f""" {sql_q} where {DIGEST_COL}=%s; """
            found_prj = self.run_sql_fetchone(sql_q, digest)

        else:
            _LOGGER.error(
                "You haven't provided neither namespace/name, digest nor id! Execution is unsuccessful"
            )
            _LOGGER.info("Files haven't been downloaded, returning empty dict")
            return {}

        _LOGGER.info(f"Project has been found: {found_prj[0]}")

        anno_dict = {
            ID_COL: found_prj[0],
            NAMESPACE_COL: found_prj[1],
            NAME_COL: found_prj[2],
            ANNO_COL: found_prj[3],
        }

        return anno_dict

    def _get_namespace_proj_anno(self, namespace: str = None) -> dict:
        """
        Get list of all project annotations in namespace
        :param str namespace: namespace
        return: dict of dicts with all projects in namespace
        """

        if not namespace:
            _LOGGER.info(f"No namespace provided... returning empty list")
            return {}

        sql_q = f"""select 
                    {ID_COL}, 
                    {NAMESPACE_COL},
                    {NAME_COL},
                    {ANNO_COL} 
                        from {DB_TABLE_NAME} where namespace='{namespace}';"""

        results = self.run_sql_fetchall(sql_q)
        res_dict = {}
        for result in results:
            res_dict[result[2]] = {
                ID_COL: result[0],
                NAMESPACE_COL: result[1],
                ANNO_COL: result[3],
            }

        return res_dict

    def run_sql_fetchone(self, sql_query: str, argv=()) -> list:
        """
        Fetching one result by providing sql query and arguments
        :param str sql_query: sql string that has to run
        :param argv: arguments that has to be added to sql query
        :return: set of query result
        """
        # coerce argv to tuple if not
        if isinstance(argv, list):
            argv = tuple(argv)
        elif not isinstance(argv, tuple):
            argv = (argv,)
        cursor = self.postgresConnection.cursor()
        try:
            cursor.execute(sql_query, argv)
            output_result = cursor.fetchone()

            # must run check here since None is not iterable.
            if output_result is not None:
                return list(output_result)
            else:
                return None
        except psycopg2.Error as e:
            _LOGGER.error(f"Error occurred while running query: {e}")
        finally:
            cursor.close()

    def run_sql_fetchall(self, sql_query: str, argv=()) -> list:
        """
        Fetching all result by providing sql query and arguments
        :param str sql_query: sql string that has to run
        :param argv: arguments that has to be added to sql query
        :return: set of query result
        """
        # coerce argv to tuple if not
        if isinstance(argv, list):
            argv = tuple(argv)
        elif not isinstance(argv, tuple):
            argv = (argv,)
        cursor = self.postgresConnection.cursor()
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
        :param dict project_dict: project dict
        :return: digest string
        """
        _LOGGER.info(f"Creating digest for: {project_dict['name']}")
        sample_digest = md5(
            json.dumps(project_dict["_samples"], sort_keys=True).encode("utf-8")
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
        result = self.run_sql_fetchall(a)
        cols_name = []
        for col in result:
            cols_name.append(col[3])
        DB_COLUMNS.sort()
        cols_name.sort()
        if DB_COLUMNS != cols_name:
            raise SchemaError