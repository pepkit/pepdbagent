from typing import Union, List

from .base import BaseConnection
from .const import DEFAULT_LIMIT, DEFAULT_OFFSET
from .models import NamespaceResultModel, NamespaceReturnModel


class PEPDatabaseNamespace:
    """
    Class that represents project Namespaces in Database.

    While using this class, user can retrieve all necessary metadata about PEPs
    """

    def __init__(self, con: BaseConnection):
        """
        :param con: Connection to db represented by BaseConnection class object
        """
        self.con = con

    def get(
        self,
        query: str = "",
        admin: Union[List[str], str] = None,
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET
    ) -> NamespaceReturnModel:
        """

        :param query:
        :param admin:
        :param limit:
        :param offset:
        :return:
        """
        ...
