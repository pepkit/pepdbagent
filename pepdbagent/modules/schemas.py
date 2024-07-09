import datetime
import json
import logging
from typing import Dict, List, NoReturn, Union

import numpy as np
import peppy
from peppy.const import (
    CONFIG_KEY,
    SAMPLE_NAME_ATTR,
    SAMPLE_RAW_DICT_KEY,
    SAMPLE_TABLE_INDEX_KEY,
    SUBSAMPLE_RAW_LIST_KEY,
)
from sqlalchemy import Select, and_, delete, select
from sqlalchemy.exc import IntegrityError, NoResultFound
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from pepdbagent.const import (
    DEFAULT_TAG,
    DESCRIPTION_KEY,
    MAX_HISTORY_SAMPLES_NUMBER,
    NAME_KEY,
    PEPHUB_SAMPLE_ID_KEY,
    PKG_NAME,
)
from pepdbagent.db_utils import (
    BaseEngine,
    HistoryProjects,
    HistorySamples,
    Projects,
    Samples,
    Subsamples,
    UpdateTypes,
    User,
)
from pepdbagent.exceptions import (
    HistoryNotFoundError,
    PEPDatabaseAgentError,
    ProjectDuplicatedSampleGUIDsError,
    ProjectNotFoundError,
    ProjectUniqueNameError,
    SampleTableUpdateError,
)
from pepdbagent.models import (
    HistoryAnnotationModel,
    HistoryChangeModel,
    ProjectDict,
    UpdateItems,
    UpdateModel,
)
from pepdbagent.utils import create_digest, generate_guid, order_samples, registry_path_converter

_LOGGER = logging.getLogger(PKG_NAME)


class PEPDatabaseSchemas:
    """
    Class that represents Schemas in Database.

    While using this class, user can create, retrieve, delete, and update schemas from database
    """

    def __init__(self, pep_db_engine: BaseEngine):
        """
        :param pep_db_engine: pepdbengine object with sa engine
        """
        self._sa_engine = pep_db_engine.engine
        self._pep_db_engine = pep_db_engine

    def get(self, namespace: str, name: str) -> Dict:
        """
        Get schema from the database.

        :param namespace: user namespace
        :param name: schema name

        :return: schema dict
        """
        ...

    def search(self, namespace: str = None, query: str = "") -> ...:
        """
        Search schemas in the database.

        :param namespace: user namespace [Default: None]. If None, search in all namespaces
        :param query: query string. [Default: ""]. If empty, return all schemas

        :return: list of schema dicts
        """
        ...

    def create(
        self,
        namespace: str,
        name: str,
        schema: dict,
        description: str = "",
        # private: bool = False, # TODO: for simplicity was not implemented yet
        overwrite: bool = False,
        update_only: bool = False,
    ) -> None:
        """
        Create or update schema in the database.

        :param namespace: user namespace
        :param name: schema name
        :param schema: schema dict
        :param description: schema description [Default: ""]
        :param overwrite: overwrite schema if exists [Default: False]
        :param update_only: update only schema if exists [Default: False]
        """
        ...

    def update(
        self,
        namespace: str,
        name: str,
        schema: dict,
        description: str = "",
        # private: bool = False, # TODO: for simplicity was not implemented yet
    ) -> None:
        """
        Update schema in the database.

        :param namespace: user namespace
        :param name: schema name
        :param schema: schema dict
        :param description: schema description [Default: ""]

        :return: None
        """
        ...

    def delete(self, namespace: str, name: str) -> None:
        """
        Delete schema from the database.

        :param namespace: user namespace
        :param name: schema name

        :return: None
        """
        ...

    def exist(self, namespace: str, name: str) -> bool:
        """
        Check if schema exists in the database.

        :param namespace: user namespace
        :param name: schema name

        :return: True if schema exists, False otherwise
        """
        ...

    def group_create(self, namespace: str, name: str, description: str = "") -> None:
        """
        Create schema group in the database.

        :param namespace: user namespace
        :param name: schema group name
        :param description: schema group description [Default: ""]
        """
        ...

    def group_get(self, namespace: str, name: str) -> ...:
        """
        Get schema group from the database.

        :param namespace: user namespace
        :param name: schema group name

        :return: ...
        """
        ...

    def group_search(self, namespace: str = None, query: str = "") -> ...:
        """
        Search schema groups in the database.

        :param namespace: user namespace [Default: None]. If None, search in all namespaces
        :param query: query string. [Default: ""]. If empty, return all schema groups

        :return: list of schema group dicts
        """
        ...

    def group_delete(self, namespace: str, name: str) -> None:
        """
        Delete schema group from the database.

        :param namespace: user namespace
        :param name: schema group name

        :return: None
        """
        ...

    def group_add_schema(
        self, namespace: str, name: str, schema_namespace: str, schema_name: str
    ) -> None:
        """
        Add schema to the schema group.

        :param namespace: user namespace
        :param name: schema group name
        :param schema_namespace: schema namespace
        :param schema_name: schema name

        :return: None
        """
        ...

    def group_remove_schema(
        self, namespace: str, name: str, schema_namespace: str, schema_name: str
    ) -> None:
        """
        Remove schema from the schema group.

        :param namespace: user namespace
        :param name: schema group name
        :param schema_namespace: schema namespace
        :param schema_name: schema name

        :return: None
        """
        ...
