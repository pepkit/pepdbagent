from psycopg2.errors import UniqueViolation
from pepdbagent.pepdbagent import PEPDatabaseAgent
from pepdbagent.models import BaseModel
import json
import psycopg2


def test_connection_initializes_correctly_from_dsn(
    mocker, sql_output_for_check_conn_db, test_dsn
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
    mocker, sql_output_for_check_conn_db
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


def test_upload_project_success(
    mocker, sql_output_for_check_conn_db, test_dsn, test_peppy_project
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

    c.project.submit(
        test_peppy_project,
        test_namespace,
    )

    assert database_commit_mock.called


def test_upload_project_updates_after_raising_unique_violation_error(
    mocker, sql_output_for_check_conn_db, test_dsn, test_peppy_project
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
    mocker, test_dsn, test_peppy_project, sql_output_for_check_conn_db
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
    mocker, test_dsn, test_peppy_project, sql_output_for_check_conn_db
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

    c.project.edit(
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


def test_delete_project(mocker, test_dsn, sql_output_for_check_conn_db):
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


def test_get_project_by_registry_path(mocker, test_dsn, sql_output_for_check_conn_db):
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
    mocker, test_dsn, sql_output_for_check_conn_db, test_database_project_return
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


def test_search_project():
    pass


def test_search_namespace():
    pass


def test_get_projects_in_namespace():
    pass


def test_get_namespace_info():
    pass


def test_get_namespaces_info_by_list():
    pass


def test_get_project_annotation():
    pass


def test_get_project_annotation_by_registry_path():
    pass


def test_get_namespace_annotation():
    pass


def test_project_exists():
    pass


def test_project_exists_by_registry_path():
    pass
