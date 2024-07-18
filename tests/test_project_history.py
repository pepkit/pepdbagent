import peppy
import pytest

from pepdbagent.const import PEPHUB_SAMPLE_ID_KEY
from pepdbagent.exceptions import HistoryNotFoundError

from .utils import PEPDBAgentContextManager


@pytest.mark.skipif(
    not PEPDBAgentContextManager().db_setup(),
    reason="DB is not setup",
)
class TestProjectHistory:
    """
    Test project methods
    """

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_get_add_history_all_annotation(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            prj = agent.project.get(namespace, name, tag="default", with_id=True)

            prj["_sample_dict"][0]["sample_name"] = "new_sample_name"

            del prj["_sample_dict"][1]
            del prj["_sample_dict"][2]
            new_sample1 = {
                "sample_name": "new_sample",
                "protocol": "new_protocol",
                PEPHUB_SAMPLE_ID_KEY: None,
            }
            new_sample2 = {
                "sample_name": "new_sample2",
                "protocol": "new_protocol2",
                PEPHUB_SAMPLE_ID_KEY: None,
            }

            prj["_sample_dict"].append(new_sample1.copy())
            prj["_sample_dict"].append(new_sample2.copy())

            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(prj)},
            )

            project_history = agent.project.get_history(namespace, name, tag="default")

            assert len(project_history.history) == 1

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_get_add_history_all_project(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            prj_init = agent.project.get(namespace, name, tag="default", raw=False)
            prj = agent.project.get(namespace, name, tag="default", with_id=True)

            # prj["_sample_dict"][0]["sample_name"] = "new_sample_name"

            del prj["_sample_dict"][1]
            del prj["_sample_dict"][2]
            new_sample1 = {
                "sample_name": "new_sample",
                "protocol": "new_protocol",
                PEPHUB_SAMPLE_ID_KEY: None,
            }
            new_sample2 = {
                "sample_name": "new_sample2",
                "protocol": "new_protocol2",
                PEPHUB_SAMPLE_ID_KEY: None,
            }

            prj["_sample_dict"].append(new_sample1.copy())
            prj["_sample_dict"].append(new_sample2.copy())

            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(prj)},
            )

            history_prj = agent.project.get_project_from_history(
                namespace, name, tag="default", history_id=1, raw=False
            )
            assert prj_init == history_prj

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_get_history_multiple_changes(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            prj = agent.project.get(namespace, name, tag="default", with_id=True)

            del prj["_sample_dict"][1]

            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(prj)},
            )

            prj = agent.project.get(namespace, name, tag="default", with_id=True)

            new_sample1 = {
                "sample_name": "new_sample",
                "protocol": "new_protocol",
                PEPHUB_SAMPLE_ID_KEY: None,
            }
            prj["_sample_dict"].append(new_sample1.copy())

            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(prj)},
            )

            history = agent.project.get_history(namespace, name, tag="default")

            assert len(history.history) == 2

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_get_project_incorrect_history_id(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            prj = agent.project.get(namespace, name, tag="default", with_id=True)
            del prj["_sample_dict"][1]

            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(prj)},
            )

            with pytest.raises(HistoryNotFoundError):
                agent.project.get_project_from_history(
                    namespace, "amendments2", tag="default", history_id=1, raw=False
                )

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_get_history_none(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            history_annot = agent.project.get_history(namespace, name, tag="default")
            assert len(history_annot.history) == 0

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_delete_all_history(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            prj = agent.project.get(namespace, name, tag="default", with_id=True)

            del prj["_sample_dict"][1]

            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(prj)},
            )

            prj = agent.project.get(namespace, name, tag="default", with_id=True)

            new_sample1 = {
                "sample_name": "new_sample",
                "protocol": "new_protocol",
                PEPHUB_SAMPLE_ID_KEY: None,
            }
            prj["_sample_dict"].append(new_sample1.copy())

            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(prj)},
            )

            history = agent.project.get_history(namespace, name, tag="default")

            assert len(history.history) == 2

            agent.project.delete_history(namespace, name, tag="default", history_id=None)

            history = agent.project.get_history(namespace, name, tag="default")
            assert len(history.history) == 0

            project_exists = agent.project.exists(namespace, name, tag="default")
            assert project_exists

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_delete_one_history(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            prj = agent.project.get(namespace, name, tag="default", with_id=True)

            del prj["_sample_dict"][1]

            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(prj)},
            )

            prj = agent.project.get(namespace, name, tag="default", with_id=True)

            new_sample1 = {
                "sample_name": "new_sample",
                "protocol": "new_protocol",
                PEPHUB_SAMPLE_ID_KEY: None,
            }
            prj["_sample_dict"].append(new_sample1.copy())

            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(prj)},
            )

            history = agent.project.get_history(namespace, name, tag="default")

            assert len(history.history) == 2

            agent.project.delete_history(namespace, name, tag="default", history_id=1)

            history = agent.project.get_history(namespace, name, tag="default")

            assert len(history.history) == 1
            assert history.history[0].change_id == 2

    @pytest.mark.parametrize(
        "namespace, name, sample_name",
        [
            ["namespace1", "amendments1", "pig_0h"],
        ],
    )
    def test_restore_project(self, namespace, name, sample_name):
        with PEPDBAgentContextManager(add_data=True) as agent:
            prj_org = agent.project.get(namespace, name, tag="default", with_id=False)
            prj = agent.project.get(namespace, name, tag="default", with_id=True)

            del prj["_sample_dict"][1]

            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(prj)},
            )

            prj = agent.project.get(namespace, name, tag="default", with_id=True)

            new_sample1 = {
                "sample_name": "new_sample",
                "protocol": "new_protocol",
                PEPHUB_SAMPLE_ID_KEY: None,
            }
            prj["_sample_dict"].append(new_sample1.copy())

            agent.project.update(
                namespace=namespace,
                name=name,
                tag="default",
                update_dict={"project": peppy.Project.from_dict(prj)},
            )

            agent.project.restore(namespace, name, tag="default", history_id=1)

            restored_project = agent.project.get(namespace, name, tag="default", with_id=False)

            assert prj_org == restored_project
