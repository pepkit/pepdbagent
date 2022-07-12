import psycopg2
import json
import logmuse
import sys
import peppy
import os
from hashlib import md5
import ubiquerg

# from pprint import pprint

DB_TABLE_NAME = "projects"
ID_COL = 'id'
PROJ_COL = 'project_value'
ANNO_COL = 'anno_info'
NAMESPACE_COL = 'namespace'
NAME_COL = 'name'
DIGEST_COL = 'digest'

DB_COLUMNS = [ID_COL, PROJ_COL, ANNO_COL, NAMESPACE_COL, NAME_COL, DIGEST_COL]

_LOGGER = logmuse.init_logger("pepDB_connector")


# TODO: create constant variables for col names and some peppy variables

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

        try:
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

        except psycopg2.Error as e:
            _LOGGER.error(f"Error occurred while connecting to db {e}")
            sys.exit("Exiting")

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

    def upload_project(self, project: peppy, namespace=None) -> None:
        cursor = self.postgresConnection.cursor()
        try:
            if namespace is None:
                namespace = "other"
            proj_dict = project.to_dict(extended=True)
            proj_name = proj_dict["name"]
            proj_digest = self._create_digest(proj_dict)
            anno_info = json.dumps(
                {"proj_description": proj_dict["description"],
                 "n_samples": len(project.samples)}
            )

            proj_dict = json.dumps(proj_dict)

            sql = """INSERT INTO projects(namespace, name, digest, project_value, anno_info)
            VALUES (%s, %s, %s, %s, %s) RETURNING id;"""
            cursor.execute(sql, (namespace,
                                 proj_name,
                                 proj_digest,
                                 proj_dict,
                                 anno_info,
                                 ))

            proj_id = cursor.fetchone()[0]

            self._commit_connection()
            cursor.close()
            _LOGGER.info(f"Project: {proj_name} was successfully uploaded. The Id of this project is {proj_id}")

        except psycopg2.Error as e:
            print(f"{e}")
            cursor.close()

    def get_project(
            self,
            registry: str = None,
            namespace: str = None,
            name: str = None,
            id: int = None
    ) -> peppy.Project:
        """
        Retrieving project from database by specifying project name or id
        :param str registry: project registry
        :param str namespace: project registry [should be used with name]
        :param str name: project name in database [should be used with namespace]
        :param str id: project id in database
        :return: peppy object with found project
        """
        sql_q = """
                select name, project_value from projects
                """
        if registry is not None:
            reg = ubiquerg.parse_registry_path(registry)
            namespace = reg['namespace']
            name = reg['item']

        if name is not None and namespace is not None:
            sql_q = f""" {sql_q} where name=%s and namespace=%s;"""
            found_prj = self.run_sql_search_single(sql_q, name, namespace)

        elif id is not None:
            sql_q = f""" {sql_q} where id=%s; """
            found_prj = self.run_sql_search_single(sql_q, id)

        else:
            _LOGGER.error(
                "You haven't provided neither name nor id! Execution is unsuccessful"
            )
            _LOGGER.info("Files haven't been downloaded, returning empty project")
            return peppy.Project()

        _LOGGER.info(f"Project has been found: {found_prj[0]}")
        project_value = found_prj[1]

        new_project = peppy.Project(project_dict=project_value)

        return new_project

    def get_projects(self, namespace: str = None) -> dict:
        """
        Get list of all projects in namespace
        return: dict with all projects in namespace
        """

        if not namespace:
            _LOGGER.info(f"No namespace provided... returning empty list")
            return {}

        sql_q = f"""select {NAME_COL}, {PROJ_COL} from {DB_TABLE_NAME} where namespace='{namespace}';"""

        results = self.run_sql_search_all(sql_q)
        res_dict = {}
        for result in results:
            res_dict[result[0]] = peppy.Project(project_dict=result[1])

        return res_dict

    def get_namespaces(self) -> list:
        """
        Get list of all available namespaces
        :return: list of available namespaces
        """
        sql_query = f"""SELECT DISTINCT namespace FROM {DB_TABLE_NAME};"""
        namespace_list = []
        try:
            for namespace in self.run_sql_search_all(sql_query):
                namespace_list.append(namespace[0])
        except KeyError:
            _LOGGER.warning("Error while getting list of namespaces")
        return namespace_list

    def run_sql_search_single(self, sql_query: str, *argv) -> list:
        """
        Fetching one result by providing sql query and arguments
        :param str sql_query: sql string that has to run
        :param argv: arguments that has to be added to sql query
        :return: set of query result
        """
        cursor = self.postgresConnection.cursor()
        try:
            cursor.execute(sql_query, argv)
            output_result = cursor.fetchone()
            cursor.close()
            return list(output_result)
        except psycopg2.Error as e:
            _LOGGER.error(f"Error occurred while running query: {e}")
        finally:
            cursor.close()

    def run_sql_search_all(self, sql_query: str, *argv) -> list:
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
        :param dict project_dict: project dict
        :return: digest string
        """
        _LOGGER.info(f"Creating digest for: {project_dict['name']}")
        sample_digest = md5(json.dumps(project_dict["_samples"], sort_keys=True).encode('utf-8')).hexdigest()

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
        result = self.run_sql_search_all(a)
        cols_name = []
        for col in result:
            cols_name.append(col[3])
        DB_COLUMNS.sort()
        cols_name.sort()
        if DB_COLUMNS != cols_name:
            raise psycopg2.Error


def main():
    # Create connection to db:
    # projectDB = PepAgent(
    #     user="postgres",
    #     password="docker",
    # )
    projectDB = PepAgent("postgresql://postgres:docker@localhost:5432/pep-base-sql")

    # Add new project to database
    prp_project2 = peppy.Project("/home/bnt4me/Virginia/pephub_db/sample_pep/subtable3/project_config.yaml").to_dict(extended=True)
    # projectDB.upload_project(prp_project2, namespace="Bruno")

    # new_pr = peppy.Project(project_dict=prp_project2)

    # print(new_pr)
    # directory = "/home/bnt4me/Virginia/pephub_db/sample_pep/"
    # os.walk(directory)
    # projects = ([os.path.join(x[0],'project_config.yaml') for x in os.walk(directory)])[1:]
    #
    # print(projects)
    # for d in projects:
    #     try:
    #         prp_project2 = peppy.Project(d)
    #         projectDB.upload_project(prp_project2)
    #     except Exception:
    #         pass
    # Get project by id:
    # pr_ob = projectDB.get_project(registry="other/imply")
    # print(pr_ob.samples)
    #
    # # #Get project by name
    # pr_ob = projectDB.get_project(project_name="imply")
    # print(pr_ob.samples)
    #
    # # Get list of available projects:
    # list_of_projects = projectDB.get_projects_list()
    # print(list_of_projects)
    # peppy.Project("/home/bnt4me/Virginia/pephub_db/sample_pep/subtable2/project_config.yaml")
    # print()
    dfd = projectDB.get_projects('other')
    dfd
    # print(projectDB.get_namespaces())


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Pipeline aborted.")
        sys.exit(1)
