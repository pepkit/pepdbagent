from datetime import datetime

import pytest

from pepdbagent.models import TarNamespaceModel

from .utils import PEPDBAgentContextManager


@pytest.mark.skipif(
    not PEPDBAgentContextManager().db_setup(),
    reason="DB is not setup",
)
class TestGeoTar:
    """
    Test project methods
    """

    test_namespace = "namespace1"

    tar_info = TarNamespaceModel(
        namespace=test_namespace,
        submission_date=datetime.now(),
        start_period=datetime.now(),
        end_period=datetime.now(),
        number_of_projects=1,
        file_path="blabla/test.tar",
    )

    def test_create_meta_tar(self):
        with PEPDBAgentContextManager(add_data=True) as agent:

            agent.namespace.upload_tar_info(tar_info=self.tar_info)

            result = agent.namespace.get_tar_info(namespace=self.test_namespace)

            assert result.count == 1

    def test_delete_meta_tar(self):
        with PEPDBAgentContextManager(add_data=True) as agent:
            agent.namespace.upload_tar_info(tar_info=self.tar_info)

            result = agent.namespace.get_tar_info(namespace=self.test_namespace)
            assert result.count == 1

            agent.namespace.delete_tar_info()

            result = agent.namespace.get_tar_info(namespace=self.test_namespace)
            assert result.count == 0
