from psycopg2.errors import UniqueViolation
from pepdbagent.pepdbagent import PEPDatabaseAgent
from pepdbagent.models import BaseModel
import json
import psycopg2
import pytest
import datetime


class TestBaseConnection:
    """
    Test connections to the database
    """

    def test_connection_initializes_correctly_from_dsn(
        self, mocker, sql_output_for_check_conn_db, test_dsn
    ):
        mocker.patch("psycopg2.connect")
        mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection.run_sql_fetchall",
            return_value=sql_output_for_check_conn_db,
        )

        c = PEPDatabaseAgent(dsn=test_dsn)

        assert c.connection.db_name == "pep-base-sql"
        assert c.connection.pg_connection.autocommit

    def test_connection_initializes_correctly_without_dsn(
        self, mocker, sql_output_for_check_conn_db
    ):
        mocker.patch("psycopg2.connect")
        mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection.run_sql_fetchall",
            return_value=sql_output_for_check_conn_db,
        )

        c = PEPDatabaseAgent(
            host="localhost",
            port="5432",
            database="pep-base-sql",
            user="postgres",
            password="docker",
        )

        assert c.connection.db_name == "pep-base-sql"
        assert c.connection.pg_connection.autocommit


class TestProject:
    def test_upload_project_success(
        selfm, mocker, sql_output_for_check_conn_db, test_dsn, test_peppy_project
    ):
        database_commit_mock = mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection.commit_to_database"
        )
        mocker.patch("psycopg2.connect")
        mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection.run_sql_fetchall",
            return_value=sql_output_for_check_conn_db,
        )
        c = PEPDatabaseAgent(dsn=test_dsn)

        test_namespace = "test"

        c.project.create(test_peppy_project, test_namespace)

        assert database_commit_mock.called

    def test_upload_project_updates_after_raising_unique_violation_error(
        self, mocker, sql_output_for_check_conn_db, test_dsn, test_peppy_project
    ):
        update_project_mock = mocker.patch(
            "pepdbagent.pepdbagent.PEPDatabaseProject._overwrite"
        )
        mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection.run_sql_fetchall",
            return_value=sql_output_for_check_conn_db,
        )
        mocker.patch("psycopg2.connect")

        mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection.commit_to_database",
            side_effect=UniqueViolation(),
        )

        c = PEPDatabaseAgent(dsn=test_dsn)
        test_namespace = "test"
        c.project._overwrite(test_peppy_project, test_namespace, overwrite=True)

        assert update_project_mock.called

    def test_update_project(
        self, mocker, test_dsn, test_peppy_project, sql_output_for_check_conn_db
    ):
        mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection.run_sql_fetchall",
            return_value=sql_output_for_check_conn_db,
        )
        mocker.patch(
            "pepdbagent.pepdbagent.PEPDatabaseProject.exists",
            return_value=True,
        )
        database_commit_mock = mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection.commit_to_database"
        )
        mocker.patch("psycopg2.connect")

        c = PEPDatabaseAgent(dsn=test_dsn)

        test_proj_dict = test_peppy_project.to_dict(extended=True)
        test_proj_dict = json.dumps(test_proj_dict)

        c.project._overwrite(
            test_proj_dict,
            namespace="test",
            proj_name="test",
            tag="test",
            project_digest="aaa",
            number_of_samples=5,
        )

        assert database_commit_mock.called

    def test_update_item(
        self, mocker, test_dsn, test_peppy_project, sql_output_for_check_conn_db
    ):
        mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection.run_sql_fetchall",
            return_value=sql_output_for_check_conn_db,
        )
        mocker.patch(
            "pepdbagent.pepdbagent.PEPDatabaseProject.exists",
            return_value=True,
        )
        database_commit_mock = mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection.commit_to_database"
        )
        mocker.patch("psycopg2.connect")

        c = PEPDatabaseAgent(dsn=test_dsn)

        test_peppy_project.description = "This is test description"

        c.project.update(
            update_dict={
                "tag": "new_tag",
                "is_private": True,
                "project": test_peppy_project,
            },
            namespace="test",
            name="test",
            tag="tag",
        )

        assert database_commit_mock.called

    def test_delete_project(self, mocker, test_dsn, sql_output_for_check_conn_db):
        mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection.run_sql_fetchall",
            return_value=sql_output_for_check_conn_db,
        )
        mocker.patch(
            "pepdbagent.pepdbagent.PEPDatabaseProject.exists",
            return_value=True,
        )

        database_commit_mock = mocker.patch("psycopg2.connect")

        mocker.patch("psycopg2.connect")

        c = PEPDatabaseAgent(dsn=test_dsn)

        ret = c.project.delete(namespace="test", name="test", tag="test")

        assert ret is None

    def test_get_project_by_registry_path(
        self, mocker, test_dsn, sql_output_for_check_conn_db
    ):
        mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection.run_sql_fetchall",
            return_value=sql_output_for_check_conn_db,
        )
        get_project_mock = mocker.patch(
            "pepdbagent.pepdbagent.PEPDatabaseProject.get",
            return_value=sql_output_for_check_conn_db,
        )
        mocker.patch("psycopg2.connect")

        c = PEPDatabaseAgent(dsn=test_dsn)

        c.project.get_by_rp("some/project:tag")

        get_project_mock.assert_called_with(
            namespace="some", name="project", tag="tag", raw=False
        )

    def test_get_project(
        self,
        mocker,
        test_dsn,
        sql_output_for_check_conn_db,
        test_database_project_return,
    ):
        mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection.run_sql_fetchall",
            return_value=sql_output_for_check_conn_db,
        )
        mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection.run_sql_fetchone",
            return_value=test_database_project_return,
        )
        mocker.patch("psycopg2.connect")

        c = PEPDatabaseAgent(dsn=test_dsn)

        project = c.project.get(
            namespace="test_namespace",
            name="test_name",
            tag="test_tag",
        )

        assert project.name == "public_project"
        assert not project.description

    def test_project_exists(
        self,
    ):
        pass


class TestAnnotation:
    """
    Test function within annotation class
    """

    @pytest.fixture(scope="function")
    def initiate_con(
        self,
        mocker,
        test_dsn,
    ):
        mocker.patch("psycopg2.connect")
        mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection._check_conn_db",
            return_value=True,
        )
        instance = PEPDatabaseAgent(dsn=test_dsn)

        yield instance

    def test_get_anno_by_providing_list(self, initiate_con, mocker):
        get_single_annot_mock = mocker.patch(
            "pepdbagent.pepdbagent.PEPDatabaseAnnotation._get_single_annotation",
        )
        initiate_con.annotation.get_by_rp(["this/is:one", "This/if:two"])
        assert get_single_annot_mock.called

    def test_get_annotation_of_single_project(self, mocker, initiate_con):
        run_sql_one_mock = mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection.run_sql_fetchone",
            return_value=[
                "1",
                "2",
                "3",
                False,
                5,
                6,
                datetime.datetime.now(),
                datetime.datetime.now(),
                "9",
                "10",
            ],
        )
        initiate_con.annotation.get("test", "project", "pr")
        assert run_sql_one_mock.called

    def test_get_annotation_of_single_project_by_rp(self, mocker, initiate_con):
        run_sql_one_mock = mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection.run_sql_fetchone",
            return_value=[
                "1",
                "2",
                "3",
                False,
                5,
                6,
                datetime.datetime.now(),
                datetime.datetime.now(),
                "9",
                "10",
            ],
        )
        initiate_con.annotation.get_by_rp("test/project:pr")
        assert run_sql_one_mock.called

    def test_get_annotation_within_namespace(self, mocker, initiate_con):
        run_sql_one_mock = mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection.run_sql_fetchall",
            return_value=[
                (
                    "1",
                    "2",
                    "3",
                    6,
                    "5",
                    "dgs",
                    False,
                    datetime.datetime.now(),
                    datetime.datetime.now(),
                ),
                (
                    "1",
                    "5",
                    "3",
                    6,
                    "5",
                    "dgs",
                    False,
                    datetime.datetime.now(),
                    datetime.datetime.now(),
                ),
            ],
        )
        count_prj_mock = mocker.patch(
            "pepdbagent.pepdbagent.PEPDatabaseAnnotation._count_projects",
            return_value=2,
        )
        f = initiate_con.annotation.get(namespace="1")
        assert f.count == 2
        assert len(f.results) == 2

    def test_get_annotation_by_providing_query(self, mocker, initiate_con):
        run_sql_one_mock = mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection.run_sql_fetchall",
            return_value=[
                (
                    "1",
                    "2",
                    "3",
                    6,
                    "5",
                    "dgs",
                    False,
                    datetime.datetime.now(),
                    datetime.datetime.now(),
                ),
                (
                    "1",
                    "5",
                    "3",
                    6,
                    "5",
                    "dgs",
                    False,
                    datetime.datetime.now(),
                    datetime.datetime.now(),
                ),
            ],
        )
        count_prj_mock = mocker.patch(
            "pepdbagent.pepdbagent.PEPDatabaseAnnotation._count_projects",
            return_value=2,
        )
        f = initiate_con.annotation.get(query="1")
        assert f.count == 2
        assert len(f.results) == 2

    def test_registry_path_exception_pass(self, initiate_con):
        initiate_con.annotation.get_by_rp(["this/is:one", "This/is/f:two"])

    def test_registry_paths_exception(self, initiate_con):
        with pytest.raises(Exception):
            initiate_con.annotation.get_by_rp("This/is/wrong:registry")


class TestNamespace:
    """
    Test function within namespace class
    """

    @pytest.fixture(scope="function")
    def initiate_con(
        self,
        mocker,
        test_dsn,
    ):
        mocker.patch("psycopg2.connect")
        mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection._check_conn_db",
            return_value=True,
        )
        instance = PEPDatabaseAgent(dsn=test_dsn)

        yield instance

    def test_get_namespace_by_providing_query(self, mocker, initiate_con):
        run_sql_one_mock = mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection.run_sql_fetchall",
            return_value=[("names", 2, 3)],
        )
        count_prj_mock = mocker.patch(
            "pepdbagent.pepdbagent.PEPDatabaseNamespace._count_namespace",
            return_value=2,
        )
        f = initiate_con.namespace.get(query="1")
        assert len(f.results) == 1

    def test_get_all_namespaces(self, mocker, initiate_con):
        run_sql_one_mock = mocker.patch(
            "pepdbagent.pepdbagent.BaseConnection.run_sql_fetchall",
            return_value=[("names", 2, 3)],
        )
        count_prj_mock = mocker.patch(
            "pepdbagent.pepdbagent.PEPDatabaseNamespace._count_namespace",
            return_value=2,
        )
        f = initiate_con.namespace.get()
        assert len(f.results) == 1
