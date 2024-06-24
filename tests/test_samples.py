import os
import warnings

import peppy
import pytest
from sqlalchemy.exc import OperationalError

import pepdbagent
from pepdbagent.exceptions import SampleNotFoundError

from .conftest import DNS

DATA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests",
    "data",
)


def db_setup():
    # Check if the database is setup
    try:
        pepdbagent.PEPDatabaseAgent(dsn=DNS)
    except OperationalError:
        warnings.warn(
            UserWarning(
                f"Skipping tests, because DB is not setup. {DNS}. To setup DB go to README.md"
            )
        )
        return False
    return True


@pytest.mark.skipif(
    not db_setup(),
    reason="DB is not setup",
)
class TestSamples:
    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_retrieve_one_sample(self, initiate_pepdb_con, namespace, name, sample_name):
        one_sample = initiate_pepdb_con.sample.get(namespace, name, sample_name, raw=False)
        assert isinstance(one_sample, peppy.Sample)
        assert one_sample.sample_name == sample_name

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_retrieve_raw_sample(self, initiate_pepdb_con, namespace, name, sample_name):
        one_sample = initiate_pepdb_con.sample.get(namespace, name, sample_name)
        assert isinstance(one_sample, dict)
        assert one_sample["sample_name"] == sample_name

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace2", "custom_index", "frog_1"],
        ],
    )
    def test_retrieve_sample_with_modified_sample_id(
        self, initiate_pepdb_con, namespace, name, sample_name
    ):
        one_sample = initiate_pepdb_con.sample.get(namespace, name, sample_name, raw=False)
        assert isinstance(one_sample, peppy.Sample)
        assert one_sample.sample_id == "frog_1"

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_update(self, initiate_pepdb_con, namespace, name, sample_name):
        initiate_pepdb_con.sample.update(
            namespace=namespace,
            name=name,
            tag="default",
            sample_name=sample_name,
            update_dict={"organism": "butterfly"},
        )
        one_sample = initiate_pepdb_con.sample.get(namespace, name, sample_name, raw=False)
        assert one_sample.organism == "butterfly"

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_update_sample_name(self, initiate_pepdb_con, namespace, name, sample_name):
        initiate_pepdb_con.sample.update(
            namespace=namespace,
            name=name,
            tag="default",
            sample_name=sample_name,
            update_dict={"sample_name": "butterfly"},
        )
        one_sample = initiate_pepdb_con.sample.get(namespace, name, "butterfly", raw=False)
        assert one_sample.sample_name == "butterfly"

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace2", "custom_index", "frog_1"],
        ],
    )
    def test_update_custom_sample_id(self, initiate_pepdb_con, namespace, name, sample_name):
        initiate_pepdb_con.sample.update(
            namespace=namespace,
            name=name,
            tag="default",
            sample_name=sample_name,
            update_dict={"sample_id": "butterfly"},
        )
        one_sample = initiate_pepdb_con.sample.get(namespace, name, "butterfly", raw=False)
        assert one_sample.sample_id == "butterfly"

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_add_new_attributes(self, initiate_pepdb_con, namespace, name, sample_name):
        initiate_pepdb_con.sample.update(
            namespace=namespace,
            name=name,
            tag="default",
            sample_name=sample_name,
            update_dict={"new_attr": "butterfly"},
        )
        prj = initiate_pepdb_con.project.get(namespace, name, raw=False)

        assert prj.get_sample(sample_name).new_attr == "butterfly"

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_project_timestamp_was_changed(self, initiate_pepdb_con, namespace, name, sample_name):
        annotation1 = initiate_pepdb_con.annotation.get(namespace, name, "default")
        import time

        time.sleep(0.2)
        initiate_pepdb_con.sample.update(
            namespace=namespace,
            name=name,
            tag="default",
            sample_name=sample_name,
            update_dict={"new_attr": "butterfly"},
        )
        annotation2 = initiate_pepdb_con.annotation.get(namespace, name, "default")

        assert annotation1.results[0].last_update_date != annotation2.results[0].last_update_date

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_delete_sample(self, initiate_pepdb_con, namespace, name, sample_name):
        one_sample = initiate_pepdb_con.sample.get(namespace, name, sample_name, raw=False)
        assert isinstance(one_sample, peppy.Sample)

        initiate_pepdb_con.sample.delete(namespace, name, tag="default", sample_name=sample_name)

        with pytest.raises(SampleNotFoundError):
            initiate_pepdb_con.sample.get(
                namespace, name, tag="default", sample_name=sample_name, raw=False
            )

    @pytest.mark.parametrize(
        "namespace, name, tag, sample_dict",
        [
            [
                "namespace1",
                "amendments1",
                "default",
                {
                    "sample_name": "new_sample",
                    "time": "new_time",
                },
            ],
        ],
    )
    def test_add_sample(self, initiate_pepdb_con, namespace, name, tag, sample_dict):
        prj = initiate_pepdb_con.project.get(namespace, name, raw=False)
        initiate_pepdb_con.sample.add(namespace, name, tag, sample_dict)

        prj2 = initiate_pepdb_con.project.get(namespace, name, raw=False)

        assert len(prj.samples) + 1 == len(prj2.samples)
        assert prj2.samples[-1].sample_name == sample_dict["sample_name"]

    @pytest.mark.parametrize(
        "namespace, name, tag, sample_dict",
        [
            [
                "namespace1",
                "amendments1",
                "default",
                {
                    "sample_name": "pig_0h",
                    "time": "new_time",
                },
            ],
        ],
    )
    def test_overwrite_sample(self, initiate_pepdb_con, namespace, name, tag, sample_dict):
        assert (
            initiate_pepdb_con.project.get(namespace, name, raw=False).get_sample("pig_0h").time
            == "0"
        )
        initiate_pepdb_con.sample.add(namespace, name, tag, sample_dict, overwrite=True)

        assert (
            initiate_pepdb_con.project.get(namespace, name, raw=False).get_sample("pig_0h").time
            == "new_time"
        )

    @pytest.mark.parametrize(
        "namespace, name, tag, sample_dict",
        [
            [
                "namespace1",
                "amendments1",
                "default",
                {
                    "sample_name": "new_sample",
                    "time": "new_time",
                },
            ],
        ],
    )
    def test_delete_and_add(self, initiate_pepdb_con, namespace, name, tag, sample_dict):
        prj = initiate_pepdb_con.project.get(namespace, name, raw=False)
        sample_dict = initiate_pepdb_con.sample.get(namespace, name, "pig_0h", raw=True)
        initiate_pepdb_con.sample.delete(namespace, name, tag, "pig_0h")
        initiate_pepdb_con.sample.add(namespace, name, tag, sample_dict)
        prj2 = initiate_pepdb_con.project.get(namespace, name, raw=False)
        assert prj.get_sample("pig_0h").to_dict() == prj2.get_sample("pig_0h").to_dict()
