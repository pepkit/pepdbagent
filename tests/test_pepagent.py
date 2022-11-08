from psycopg2.errors import UniqueViolation
from pepdbagent.pepdbagent import Connection
import json
from pepdbagent.pepannot import Annotation


def test_connection_initializes_correctly_from_dsn(
    mocker, sql_output_for_check_conn_db, test_dsn
):
    mocker.patch("pepdbagent.pepdbagent.psycopg2.connect")
    mocker.patch(
        "pepdbagent.pepdbagent.Connection._run_sql_fetchall",
        return_value=sql_output_for_check_conn_db,
    )

    c = Connection(dsn=test_dsn)

    assert c.db_name == "pep-base-sql"
    assert c.pg_connection.autocommit


def test_connection_initializes_correctly_without_dsn(
    mocker, sql_output_for_check_conn_db
):
    mocker.patch("pepdbagent.pepdbagent.psycopg2.connect")
    mocker.patch(
        "pepdbagent.pepdbagent.Connection._run_sql_fetchall",
        return_value=sql_output_for_check_conn_db,
    )

    c = Connection(
        host="localhost",
        port="5432",
        database="pep-base-sql",
        user="postgres",
        password="docker",
    )

    assert c.db_name == "pep-base-sql"
    assert c.pg_connection.autocommit


def test_upload_project_success(
    mocker, sql_output_for_check_conn_db, test_dsn, test_peppy_project
):
    database_commit_mock = mocker.patch(
        "pepdbagent.pepdbagent.Connection._commit_to_database"
    )
    mocker.patch("psycopg2.connect")
    mocker.patch(
        "pepdbagent.pepdbagent.Connection._run_sql_fetchall",
        return_value=sql_output_for_check_conn_db,
    )
    c = Connection(dsn=test_dsn)

    c.upload_project(test_peppy_project)

    assert database_commit_mock.called


def test_upload_project_updates_after_raising_unique_violation_error(
    mocker, sql_output_for_check_conn_db, test_dsn, test_peppy_project
):
    update_project_mock = mocker.patch(
        "pepdbagent.pepdbagent.Connection._update_project"
    )
    mocker.patch(
        "pepdbagent.pepdbagent.Connection._run_sql_fetchall",
        return_value=sql_output_for_check_conn_db,
    )
    mocker.patch("psycopg2.connect")

    mocker.patch(
        "pepdbagent.pepdbagent.Connection._commit_to_database",
        side_effect=UniqueViolation(),
    )

    c = Connection(dsn=test_dsn)

    c.upload_project(test_peppy_project, overwrite=True)

    assert update_project_mock.called


def test_update_project(
    mocker, test_dsn, test_peppy_project, sql_output_for_check_conn_db
):
    mocker.patch(
        "pepdbagent.pepdbagent.Connection._run_sql_fetchall",
        return_value=sql_output_for_check_conn_db,
    )
    mocker.patch(
        "pepdbagent.pepdbagent.Connection.project_exists",
        return_value=True,
    )
    database_commit_mock = mocker.patch(
        "pepdbagent.pepdbagent.Connection._commit_to_database"
    )
    mocker.patch("psycopg2.connect")

    c = Connection(dsn=test_dsn)

    test_proj_dict = test_peppy_project.to_dict(extended=True)
    test_proj_dict = json.dumps(test_proj_dict)

    test_proj_annot = Annotation()

    c._update_project(
        test_proj_dict,
        namespace="test",
        proj_name="test",
        tag="test",
        project_digest="aaa",
        proj_annot=test_proj_annot,
    )

    assert database_commit_mock.called


def test_get_project_by_registry_path(mocker, test_dsn, sql_output_for_check_conn_db):
    mocker.patch(
        "pepdbagent.pepdbagent.Connection._run_sql_fetchall",
        return_value=sql_output_for_check_conn_db,
    )
    get_project_mock = mocker.patch(
        "pepdbagent.pepdbagent.Connection.get_project",
        return_value=sql_output_for_check_conn_db,
    )
    mocker.patch("psycopg2.connect")

    c = Connection(dsn=test_dsn)

    c.get_project_by_registry_path("some/project:tag")

    get_project_mock.assert_called_with(namespace="some", name="project", tag="tag")


def test_get_project(
    mocker, test_dsn, sql_output_for_check_conn_db, test_database_project_return
):
    mocker.patch(
        "pepdbagent.pepdbagent.Connection._run_sql_fetchall",
        return_value=sql_output_for_check_conn_db,
    )
    mocker.patch(
        "pepdbagent.pepdbagent.Connection._run_sql_fetchone",
        return_value=test_database_project_return,
    )
    mocker.patch("psycopg2.connect")

    c = Connection(dsn=test_dsn)

    project = c.get_project(
        namespace="test_namespace",
        name="test_name",
        tag="test_tag",
    )

    assert project.name == "public_project"
    assert not project.description


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
