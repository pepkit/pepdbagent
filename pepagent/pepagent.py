from typing import List, Union
import psycopg2
from psycopg2.errors import UniqueViolation, NotNullViolation
import json
import logmuse
import peppy
from hashlib import md5
from itertools import chain
import ubiquerg
import sys
import os
import datetime

from .utils import all_elements_are_strings, is_valid_resgistry_path
from .const import *
from .exceptions import SchemaError
import coloredlogs

# from pprint import pprint

_LOGGER = logmuse.init_logger("pepDB_connector")
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] %(message)s",
)


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
        tag: str = None,
        anno: dict = None,
        update: bool = False,
    ) -> None:
        """
        Upload project to the database
        :param peppy.Project project: Project object that has to be uploaded to the DB
        :param namespace: namespace of the project (Default: 'other')
        :param name: name of the project (Default: name is taken from the project object)
        :param tag: tag (or version) of the project
        :param anno: dict with annotations about current project
        :param update: boolean value if existed project has to be updated automatically
        """
        cursor = self.postgresConnection.cursor()
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

            proj_digest = self._create_digest(proj_dict)

            # adding project status to db:
            if STATUS_KEY in anno:
                proj_status = anno[STATUS_KEY]
                del anno[STATUS_KEY]
            else:
                proj_status = DEFAULT_STATUS

            anno_info = {
                "proj_description": proj_dict["description"],
                "n_samples": len(project.samples),
                "last_update": str(datetime.datetime.now()),
                "status": proj_status,
            }
            if anno:
                anno_info.update(anno)
            anno_info = json.dumps(anno_info)
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
                        anno_info,
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
                        anno=anno,
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
                f"Error while uploading project. Project hasn't ben uploaded!"
            )
            cursor.close()

    def update_project(
        self,
        project: peppy.Project,
        namespace: str = None,
        name: str = None,
        tag: str = None,
        anno: dict = None,
    ) -> None:
        """
        Upload project to the database
        :param peppy.Project project: Project object that has to be uploaded to the DB
        :param namespace: namespace of the project (Default: 'other')
        :param name: name of the project (Default: name is taken from the project object)
        :param tag: tag (or version) of the project
        :param anno: dict with annotations about current project
        :param update: boolean value if project hase to be updated
        """
        cursor = self.postgresConnection.cursor()
        if namespace is None:
            namespace = DEFAULT_NAMESPACE
        if tag is None:
            tag = DEFAULT_TAG
        proj_dict = project.to_dict(extended=True)
        if name:
            proj_name = name
        else:
            proj_name = proj_dict["name"]

        proj_digest = self._create_digest(proj_dict)

        # adding project status to db:
        if STATUS_KEY in anno:
            proj_status = anno[STATUS_KEY]
            del anno[STATUS_KEY]
        else:
            proj_status = DEFAULT_STATUS

        anno_info = {
            "proj_description": proj_dict["description"],
            "n_samples": len(project.samples),
            "last_update": str(datetime.datetime.now()),
            "status": proj_status,
        }

        if anno:
            anno_info.update(anno)
        anno_info = json.dumps(anno_info)
        proj_dict = json.dumps(proj_dict)

        if self.check_project_existance(namespace=namespace, name=proj_name, tag=tag):
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
                        anno_info,
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

    def get_project(
        self,
        *,
        registry_path: str = None,
        namespace: str = None,
        name: str = None,
        tag: str = None,
        digest: str = None,
    ) -> peppy.Project:
        """
        Retrieving project from database by specifying project registry_path, name, or digest
        :param registry_path: project registry_path
        :param namespace: project registry_path
        :param name: project name in database
        :param tag: tag of the project
        :param digest: project digest in database
        :return: peppy object with found project
        """
        sql_q = f"""
                select {ID_COL}, {PROJ_COL} from {DB_TABLE_NAME}
                """
        if registry_path is not None:
            reg = ubiquerg.parse_registry_path(registry_path)
            namespace = reg["namespace"]
            name = reg["item"]
            tag = reg["tag"]

        if name is not None:
            if namespace is None:
                namespace = DEFAULT_NAMESPACE
            if tag is None:
                tag = DEFAULT_TAG
            sql_q = f""" {sql_q} where {NAME_COL}=%s and {NAMESPACE_COL}=%s and {TAG_COL}=%s;"""
            found_prj = self.run_sql_fetchone(sql_q, name, namespace, tag)

        elif digest is not None:
            sql_q = f""" {sql_q} where {DIGEST_COL}=%s; """
            found_prj = self.run_sql_fetchone(sql_q, digest)

        else:
            _LOGGER.error(
                "You haven't provided neither registry_path, name nor digest! Execution is unsuccessful"
                "Files haven't been downloaded, returning empty project"
            )
            return peppy.Project()

        if found_prj:
            _LOGGER.info(f"Project has been found: {found_prj[0]}")
            project_value = found_prj[1]
            return peppy.Project(project_dict=project_value)
        else:
            _LOGGER.warning(
                f"No project found for supplied input. Did you supply a valid namespace and project? {sql_q}"
            )
            return peppy.Project()

    def get_projects(
        self,
        *,
        registry_paths: Union[str, List[str]] = None,
        namespace: str = None,
        tag: str = None,
    ) -> List[peppy.Project]:
        """
        Get a list of projects as peppy.Project instances. This function can be used in 3 ways:
        1. Get all projects in the database (call empty)
        2. Get a list of projects using a list registry paths
        3. Get a list of projects in a namespace
        4. Get a list of projects with certain tag (can be used with namespace)

        :param registry_paths: A list of registry paths of the form {namespace}/{name}.
        :param namespace: The namespace to fetch all projects from.
        :param tag: The tag to fetch all projects from.
        :return: a list of peppy.Project instances for the requested projects.
        """
        # Case 1. Fetch all projects in database
        if all([registry_paths is None, namespace is None, tag is None]):
            sql_q = f"select {NAME_COL}, {PROJ_COL} from {DB_TABLE_NAME}"
            results = self.run_sql_fetchall(sql_q)

        # Case 2. fetch list of registry paths
        elif registry_paths:
            # check typing
            if all(
                [
                    not isinstance(registry_paths, str),
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
            results = self.run_sql_fetchall(sql_q, *flattened_registries)

        # Case 3. Get projects by namespace
        elif namespace:
            if tag:
                sql_q = (
                    f"select {NAME_COL}, {PROJ_COL} "
                    f"from {DB_TABLE_NAME} "
                    f"where namespace = %s and tag = %s"
                )
                results = self.run_sql_fetchall(sql_q, namespace, tag)
            else:
                sql_q = f"select {NAME_COL}, {PROJ_COL} from {DB_TABLE_NAME} where namespace = %s"
                results = self.run_sql_fetchall(sql_q, namespace)

        # Case 4. Get projects by namespace
        elif tag:
            sql_q = f"select {NAME_COL}, {PROJ_COL} from {DB_TABLE_NAME} where tag = %s"
            results = self.run_sql_fetchall(sql_q, tag)
            print(results)

        else:
            _LOGGER.warning(f"Incorrect input!")
            results = []

        # extract out the project config dictionary from the query
        return [peppy.Project(project_dict=p[1]) for p in results]

    def get_namespace(self, namespace: str) -> dict:
        """
        Fetch a particular namespace from the database. This doesn't retrieve full project
        objects. For that, one should utilize the `get_projects(namespace=...)` function.

        :param namespace: the namespace to fetch
        :return: A dictionary representation of the namespace in the database
        """
        try:
            sql_q = f"select {ID_COL}, {NAME_COL}, {TAG_COL}, {DIGEST_COL}, {ANNO_COL} from {DB_TABLE_NAME} where namespace = %s"
            results = self.run_sql_fetchall(sql_q, namespace)
            projects = [
                {
                    "id": p[0],
                    "name": p[1],
                    "tag": p[2],
                    "digest": p[3],
                    "description": p[4]["proj_description"],
                    "n_samples": p[4]["n_samples"],
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

    def get_namespaces(
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
            namespaces = [n[0] for n in self.run_sql_fetchall(sql_q)]
            if names_only:
                return [n for n in namespaces]

        namespaces_list = []
        for ns in namespaces:
            try:
                namespaces_list.append(self.get_namespace(ns))
            except TypeError:
                _LOGGER.warning(
                    f"Warning: Error in collecting projects from database. {ns} wasn't collected!"
                )

        return namespaces_list

    def get_project_annotation(
        self,
        registry_path: str = None,
        namespace: str = None,
        name: str = None,
        tag: str = None,
        digest: str = None,
    ) -> dict:
        """
        Retrieving project annotation dict by specifying project name, or digest
        Additionally you can return all namespace project annotations by specifying only namespace
        :param registry_path: project registry_path
        :param namespace: project registry_path - will return dict of project annotations
        :param name: project name in database
        :param tag: tag of the projects
        :param digest: project digest in database
        :return: dict of annotations
        """
        sql_q = f"""
                select 
                    {ID_COL}, 
                    {NAMESPACE_COL},
                    {NAME_COL},
                    {TAG_COL},
                    {ANNO_COL}
                        from {DB_TABLE_NAME}
                """
        if registry_path:
            reg = ubiquerg.parse_registry_path(registry_path)
            namespace = reg["namespace"]
            name = reg["item"]
            tag = reg["tag"]

        if not name and not tag and namespace:
            return self._get_namespace_proj_anno(namespace)

        if name and namespace and tag:
            sql_q = f""" {sql_q} where {NAME_COL}=%s and {NAMESPACE_COL}=%s and {TAG_COL}=%s;"""
            found_prj = self.run_sql_fetchone(sql_q, name, namespace, tag)

        elif name and namespace:
            sql_q = f""" {sql_q} where {NAME_COL}=%s and {NAMESPACE_COL}=%s;"""
            found_prj = self.run_sql_fetchone(sql_q, name, namespace)

        elif tag:
            sql_q = f""" {sql_q} where {TAG_COL}=%s; """
            found_prj = self.run_sql_fetchone(sql_q, tag)

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
            TAG_COL: found_prj[3],
            ANNO_COL: found_prj[4],
        }

        return anno_dict

    def get_namespace_annotation(self, namespace: str = None) -> dict:
        """
        Retrieving namespace annotation dict with number of tags, projects and samples.
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
        result = self.run_sql_fetchall(sql_q)
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

    def _get_namespace_proj_anno(self, namespace: str = None) -> dict:
        """
        Get list of all project annotations in namespace
        :param namespace: namespace
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
                TAG_COL: result[3],
                ANNO_COL: result[4],
            }

        return res_dict

    def check_project_existance(
        self,
        *,
        registry_path: str = None,
        namespace: str = DEFAULT_NAMESPACE,
        name: str = None,
        tag: str = DEFAULT_TAG,
    ) -> bool:
        """
        Checking if project exists in the database
        :param registry_path: project registry path
        :param namespace: project namespace
        :param name: project name
        :param tag: project tag
        :return: Returning True if project exist
        """
        if registry_path is not None:
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
        sql = f"""SELECT {ID_COL} from {DB_TABLE_NAME} 
                    WHERE {NAMESPACE_COL} = %s AND
                          {NAME_COL} = %s AND 
                          {TAG_COL} = %s;"""

        if self.run_sql_fetchone(sql, namespace, name, tag):
            return True
        else:
            return False

    def check_project_status(
        self,
        *,
        registry_path: str = None,
        namespace: str = None,
        name: str = None,
        tag: str = None,
    ) -> str:
        """
        Retrieve project status by providing registry path or name, namespace and tag
        :param registry_path: project registry
        :param namespace: project registry - will return dict of project annotations
        :param name: project name in database. [required if registry_path does not specify]
        :param tag: tag of the projects
        :return: status
        """
        sql_q = f"""
                select ({ANNO_COL}->>'status') as status
                        from {DB_TABLE_NAME}
                            WHERE {NAMESPACE_COL}=%s AND
                                {NAME_COL}=%s AND {TAG_COL}=%s;
                """
        if registry_path:
            reg = ubiquerg.parse_registry_path(registry_path)
            namespace = reg["namespace"]
            name = reg["item"]
            tag = reg["tag"]

        if not namespace:
            namespace = DEFAULT_NAMESPACE

        if not tag:
            tag = DEFAULT_TAG

        if not name:
            _LOGGER.error(
                "You haven't provided neither registry_path or name! Execution is unsuccessful. "
                "Files haven't been downloaded, returning empty dict"
            )
            return "None"

        if not self.check_project_existance(namespace=namespace, name=name, tag=tag):
            _LOGGER.error("Project does not exist, returning None")
            return "None"

        result = self.run_sql_fetchone(sql_q, namespace, name, tag)

        return result[0]

    def run_sql_fetchone(self, sql_query: str, *argv) -> list:
        """
        Fetching one result by providing sql query and arguments
        :param sql_query: sql string that has to run
        :param argv: arguments that has to be added to sql query
        :return: set of query result
        """
        cursor = self.postgresConnection.cursor()
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

    def run_sql_fetchall(self, sql_query: str, *argv) -> list:
        """
        Fetching all result by providing sql query and arguments
        :param str sql_query: sql string that has to run
        :param argv: arguments that has to be added to sql query
        :return: set of query result
        """
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
        :param project_dict: project dict
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


def main():
    # Create connection to db:
    # projectDB = PepAgent(
    #     user="postgres",
    #     password="docker",
    # )
    projectDB = PepAgent("postgresql://postgres:docker@localhost:5432/pep-base-sql")

    # prp_project2 = peppy.Project("/home/bnt4me/Virginia/pephub_db/sample_pep/amendments2/project_config.yaml")
    # projectDB.upload_project(prp_project2, namespace="Date", anno={"sample_anno": "Tony Stark "})

    # Add new projects to database
    # directory = "/home/bnt4me/Virginia/pephub_db/sample_pep/"
    # os.walk(directory)
    # projects = (
    #     [os.path.join(x[0], "project_config.yaml") for x in os.walk(directory)]
    # )[1:]
    #
    # print(projects)
    # for d in projects:
    #     try:
    #         prp_project2 = peppy.Project(d)
    #         projectDB.upload_project(prp_project2, namespace="other1", anno={"sample_anno": "Tony Stark ", "status": 1})
    #     except Exception:
    #         pass

    # dfd = projectDB.get_project(registry="King/amendments2")
    # print(dfd)
    # dfd = projectDB.get_projects(tag="new_tag")
    # print(dfd)
    # dfd = projectDB.get_namespaces()
    # print(dfd)
    # dfd = projectDB.get_namespace(namespace="other")
    # print(dfd)

    d = projectDB.check_project_status(registry_path="other1/subtable4:primary")

    # print(projectDB.get_namespace_annotation())


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Pipeline aborted.")
        sys.exit(1)
