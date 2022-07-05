import psycopg2
import json
import logmuse
import sys
import peppy
import os

# from pprint import pprint

_LOGGER = logmuse.init_logger("pepDB_connector")

# TODO: create constant variables for col names and some peppy variables

class PepAgent:
    """
    A class to connect to pep-db and upload, download, read and process pep projects.
    """

    def __init__(
        self,
        host="localhost",
        port=5432,
        database="pep-base-sql",
        user=None,
        password=None,
    ):
        _LOGGER.info(f"Initializing connection to {database}...")

        try:
            self.postgresConnection = psycopg2.connect(
                host=host, port=port, database=database, user=user, password=password
            )

            _LOGGER.info(f"Connected!")

        except psycopg2.Error as e:
            _LOGGER.error(f"Error occurred while connecting to db {e}")
            sys.exit("Exiting")

    def _commit_connection(self):
        """
        Commit connection
        """
        self.postgresConnection.commit()

    def close_connection(self):
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
        self, project_name: str = None, project_id: int = None
    ) -> peppy.Project:
        """
        Retrieving project from database by specifying project name or id
        :param str project_name: project name in database
        :param str project_id: project id in database
        :return: peppy object with found project
        """
        sql_q = """
                select name, project_value from projects
                """

        if project_name is not None:
            sql_q = f""" {sql_q} where name=%s;"""
            found_prj = self.run_sql_search_single(sql_q, project_name)

        elif project_id is not None:
            sql_q = f""" {sql_q} where id=%s; """
            found_prj = self.run_sql_search_single(sql_q, project_id)

        else:
            _LOGGER.error(
                "You haven't provided neither name nor id! Execution is unsuccessful"
            )
            _LOGGER.info("Files haven't been downloaded, returning empty project")
            return peppy.Project()

        _LOGGER.info(f"Project has been found: {found_prj[0]}")
        project_value = found_prj[1]

        new_project = peppy.Project()
        new_project.from_dict(project_value)

        return new_project

    def get_projects_list(self) -> list:
        """
        Get list of all projects
        return: list with ids, names, and descriptions of the project
        """

        sql_q = """select id, name, anno_info from projects"""
        result = self.run_sql_search_all(sql_q)

        return result

    def run_sql_search_single(self, sql_query: str, *argv) -> set:
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
            return output_result
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
    def _create_digest(project_dict: dict):
        """
        Create digest for PEP project
        :param dict project_dict: project dict
        :return: digest string
        """
        _LOGGER.info(f"Creating digest for: {project_dict['name']}")
        return "digest007"


def main():
    # Create connection to db:
    projectDB = PepAgent(
        user="postgres",
        password="docker",
    )

    # Add new project to database
    # prp_project2 = peppy.Project("/home/bnt4me/Virginia/pephub_db/sample_pep/subtable2/project_config.yaml")
    # projectDB.upload_project(prp_project2)

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
    pr_ob = projectDB.get_project(project_id=3)
    print(pr_ob.samples)

    # #Get project by name
    pr_ob = projectDB.get_project(project_name="imply")
    print(pr_ob.samples)

    # Get list of available projects:
    list_of_projects = projectDB.get_projects_list()
    print(list_of_projects)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Pipeline aborted.")
        sys.exit(1)
