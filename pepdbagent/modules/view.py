# View of the PEP. In other words, it is a part of the PEP, or subset of the samples in the PEP.

import logging
from typing import Union
import datetime

import peppy
from peppy.const import SAMPLE_TABLE_INDEX_KEY
from sqlalchemy import select, and_, func
from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified


from pepdbagent.const import (
    DEFAULT_TAG,
    PKG_NAME,
)
from pepdbagent.exceptions import SampleNotFoundError

from pepdbagent.db_utils import BaseEngine, Samples, Projects

_LOGGER = logging.getLogger(PKG_NAME)


class PEPDatabaseView:
    """
    Class that represents Project in Database.

    While using this class, user can create, retrieve, delete, and update projects from database
    """

    def __init__(self, pep_db_engine: BaseEngine):
        """
        :param pep_db_engine: pepdbengine object with sa engine
        """
        self._sa_engine = pep_db_engine.engine
        self._pep_db_engine = pep_db_engine

    def get(
        self,
        namespace: str,
        name: str,
        tag: str = DEFAULT_TAG,
        raw: bool = False,
    ) -> Union[peppy.Project, dict, None]:
        """
        Retrieve view of the project from the database.
        View is a subset of the samples in the project. e.g. bed-db project has all the samples in bedbase,
        bedset is a view of the bedbase project with only the samples in the bedset.

        :param namespace: namespace of the project
        :param name: name of the project (Default: name is taken from the project object)
        :param raw: retrieve unprocessed (raw) PEP dict.
        :return: peppy.Project object with found project or dict with unprocessed
            PEP elements: {
                name: str
                description: str
                _config: dict
                _sample_dict: dict
                _subsample_dict: dict
            }
        """
        ...

    def create(
        self,
        namespace: str,
        view_name: str,
        sample_id_list: Union[list, tuple, str] = None,
        description: str = None,
    ):
        ...

    def delete(self):
        ...

    def add_sample(
        self,
        namespace: str,
        name: str,
        tag: str,
        sample_name: str,
        view_name: str,
    ):
        ...

    def remove_sample(self, namespace: str, view_name: str, sample_name: str):
        ...

    def get_snap_view(
        self, namespace: str, name: str, tag: str, sample_name_list: Union[list, tuple, str] = None
    ):
        """
        Get a snap view of the project. Snap view is a view of the project
        with only the samples in the list. This view won't be saved in the database.

        :param namespace:
        :param name:
        :param tag:
        :param sample_name_list:
        :return:
        """
        ...
