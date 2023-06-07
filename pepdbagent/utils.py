import json
from collections.abc import Iterable
from hashlib import md5
from typing import Tuple, Union

import ubiquerg
from peppy.const import SAMPLE_RAW_DICT_KEY

from .exceptions import RegistryPathError


def is_valid_registry_path(rpath: str) -> bool:
    """
    Verify that a registry path is valid. Checks for two things:
    1. Contains forward slash ("/"), and
    2. Forward slash divides two strings

    :param str rpath: registry path to test
    :return bool: Is it a valid registry or not.
    """
    # check for string
    if not isinstance(rpath, str):
        return False
    return all(
        [
            "/" in rpath,
            len(rpath.split("/")) == 2,
            all([isinstance(s, str) for s in rpath.split("/")]),
        ]
    )


def all_elements_are_strings(iterable: Iterable) -> bool:
    """
    Helper method to determine if an iterable only contains `str` objects.

    :param Iterable iterable: An iterable item
    :returns bool: Boolean value indicating if the iterable only contains strings.
    """
    if not isinstance(iterable, Iterable):
        return False
    return all([isinstance(item, str) for item in iterable])


def create_digest(project_dict: dict) -> str:
    """
    Create digest for PEP project

    :param project_dict: project dict
    :return: digest string
    """
    sample_digest = md5(
        json.dumps(
            project_dict[SAMPLE_RAW_DICT_KEY],
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    return sample_digest


def registry_path_converter(registry_path: str) -> Tuple[str, str, str]:
    """
    Convert registry path to namespace, name, tag

    :param registry_path: registry path that has structure: "namespace/name:tag"
    :return: tuple(namespace, name, tag)
    """
    if is_valid_registry_path(registry_path):
        reg = ubiquerg.parse_registry_path(registry_path)
        namespace = reg["namespace"]
        name = reg["item"]
        tag = reg["tag"]
        return namespace, name, tag

    raise RegistryPathError(f"Error in: '{registry_path}'")


def tuple_converter(value: Union[tuple, list, str, None]) -> tuple:
    """
    Convert string list or tuple to tuple.
    # is used to create admin tuple.

    :param value: Any value that has to be converted to tuple
    :return: tuple of strings
    """
    if isinstance(value, str):
        value = [value]
    if value:
        return tuple(value)
    return tuple(
        " ",
    )
