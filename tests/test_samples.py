import peppy
import pytest

from pepdbagent.exceptions import SampleNotFoundError

from .utils import PEPDBAgentContextManager


@pytest.mark.skipif(
    not PEPDBAgentContextManager().db_setup(),
    reason="DB is not setup",
)
class TestSamples:
    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_retrieve_one_sample(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            one_sample = agent.sample.get(namespace, name, sample_name, raw=False)
            assert isinstance(one_sample, peppy.Sample)
            assert one_sample.sample_name == sample_name

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_retrieve_raw_sample(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            one_sample = agent.sample.get(namespace, name, sample_name)
            assert isinstance(one_sample, dict)
            assert one_sample["sample_name"] == sample_name

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace2", "custom_index", "frog_1"],
        ],
    )
    def test_retrieve_sample_with_modified_sample_id(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            one_sample = agent.sample.get(namespace, name, sample_name, raw=False)
            assert isinstance(one_sample, peppy.Sample)
            assert one_sample.sample_id == "frog_1"

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_update(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.sample.update(
                namespace=namespace,
                name=name,
                tag="default",
                sample_name=sample_name,
                update_dict={"organism": "butterfly"},
            )
            one_sample = agent.sample.get(namespace, name, sample_name, raw=False)
            assert one_sample.organism == "butterfly"

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_update_sample_name(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.sample.update(
                namespace=namespace,
                name=name,
                tag="default",
                sample_name=sample_name,
                update_dict={"sample_name": "butterfly"},
            )
            one_sample = agent.sample.get(namespace, name, "butterfly", raw=False)
            assert one_sample.sample_name == "butterfly"

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace2", "custom_index", "frog_1"],
        ],
    )
    def test_update_custom_sample_id(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.sample.update(
                namespace=namespace,
                name=name,
                tag="default",
                sample_name=sample_name,
                update_dict={"sample_id": "butterfly"},
            )
            one_sample = agent.sample.get(namespace, name, "butterfly", raw=False)
            assert one_sample.sample_id == "butterfly"

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_add_new_attributes(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.sample.update(
                namespace=namespace,
                name=name,
                tag="default",
                sample_name=sample_name,
                update_dict={"new_attr": "butterfly"},
            )
            prj = agent.project.get(namespace, name, raw=False)

            assert prj.get_sample(sample_name).new_attr == "butterfly"

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_project_timestamp_was_changed(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            annotation1 = agent.annotation.get(namespace, name, "default")
            import time

            time.sleep(0.2)
            agent.sample.update(
                namespace=namespace,
                name=name,
                tag="default",
                sample_name=sample_name,
                update_dict={"new_attr": "butterfly"},
            )
            annotation2 = agent.annotation.get(namespace, name, "default")

            assert (
                annotation1.results[0].last_update_date != annotation2.results[0].last_update_date
            )

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_delete_sample(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            one_sample = agent.sample.get(namespace, name, sample_name, raw=False)
            assert isinstance(one_sample, peppy.Sample)

            agent.sample.delete(namespace, name, tag="default", sample_name=sample_name)

            with pytest.raises(SampleNotFoundError):
                agent.sample.get(
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
    def test_add_sample(self, namespace, name, tag, sample_dict):
        with PEPDBAgentContextManager(add_data=True) as agent:
            prj = agent.project.get(namespace, name, raw=False)
            agent.sample.add(namespace, name, tag, sample_dict)

            prj2 = agent.project.get(namespace, name, raw=False)

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
    def test_overwrite_sample(self, namespace, name, tag, sample_dict):
        with PEPDBAgentContextManager(add_data=True) as agent:
            assert agent.project.get(namespace, name, raw=False).get_sample("pig_0h").time == "0"
            agent.sample.add(namespace, name, tag, sample_dict, overwrite=True)

            assert (
                agent.project.get(namespace, name, raw=False).get_sample("pig_0h").time
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
    def test_delete_and_add(self, namespace, name, tag, sample_dict):
        with PEPDBAgentContextManager(add_data=True) as agent:
            prj = agent.project.get(namespace, name, raw=False)
            sample_dict = agent.sample.get(namespace, name, "pig_0h", raw=True)
            agent.sample.delete(namespace, name, tag, "pig_0h")
            agent.sample.add(namespace, name, tag, sample_dict)
            prj2 = agent.project.get(namespace, name, raw=False)
            assert prj.get_sample("pig_0h").to_dict() == prj2.get_sample("pig_0h").to_dict()
