from typing import Union, List

from .base import BaseConnection
from .const import DEFAULT_LIMIT, DEFAULT_OFFSET
from .models import AnnotationReturnModel


class PEPDatabaseAnnotation:
    """
    Class that represents project Annotations in the Database.

    While using this class, user can retrieve all necessary metadata about PEPs
    """

    def __init__(self, con: BaseConnection):
        """
        :param con: Connection to db represented by BaseConnection class object
        """
        self.con = con

    def get(
        self,
        namespace: str = None,
        name: str = None,
        tag: str = None,
        query: str = "",
        admin: Union[List[str], str] = None,
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET
    ) -> AnnotationReturnModel:
        """

        :param namespace:
        :param name:
        :param tag:
        :param query:
        :param admin:
        :param limit:
        :param offset:
        :return:
        """
        ...

    def get_by_rp(
        self,
        registry_paths: Union[List[str], str],
        admin: Union[List[str], str] = None,
        limit: int = DEFAULT_LIMIT,
        offset: int = DEFAULT_OFFSET
    ) -> AnnotationReturnModel:
        """

        :param registry_paths:
        :param admin:
        :param limit:
        :param offset:
        :return:
        """
        ...
