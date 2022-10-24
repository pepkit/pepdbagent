import peppy
import pytest


@pytest.fixture
def sql_output_for_check_conn_db():
    return [
        (None, None, None, "anno_info"),
        (None, None, None, "digest"),
        (None, None, None, "id"),
        (None, None, None, "name"),
        (None, None, None, "namespace"),
        (None, None, None, "project_value"),
        (None, None, None, "tag"),
    ]


@pytest.fixture
def test_dsn():
    return "postgresql://postgres:docker@localhost:5432/pep-base-sql"


@pytest.fixture
def test_peppy_project():
    return peppy.Project("tests/data/basic_pep/project_config.yaml")


@pytest.fixture
def test_database_project_return():
    return [
        15,
        {
            "name": "public_project",
            "_config": {
                "pep_version": "2.0.0",
                "sample_table": "/home/cgf8xr/databio/repos/example_peps/example_basic/sample_table.csv",
            },
            "description": None,
            "_sample_dict": {
                "file": {
                    "frog_1": "data/frog1_data.txt",
                    "frog_2": "data/frog2_data.txt",
                },
                "protocol": {"frog_1": "anySampleType", "frog_2": "anySampleType"},
                "sample_name": {"frog_1": "frog_1", "frog_2": "frog_2"},
            },
            "_subsample_dict": None,
        },
        {
            "status": "Unknown",
            "n_samples": 2,
            "is_private": False,
            "description": None,
            "last_update": "2022-10-24 12:24:24.210667",
        },
    ]
