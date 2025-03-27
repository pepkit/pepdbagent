import datetime
import json
import uuid
from collections.abc import Iterable
from hashlib import md5
from typing import List, Tuple, Union

import ubiquerg
from peppy.const import SAMPLE_RAW_DICT_KEY

from pepdbagent.exceptions import RegistryPathError


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


def schema_path_converter(schema_path: str) -> Tuple[str, str, str]:
    """
    Convert schema path to namespace, name

    :param schema_path: schema path that has structure: "namespace/name.yaml"
    :return: tuple(namespace, name, version)
    """
    if "/" in schema_path:
        namespace, name_tag = schema_path.split("/")
        if ":" in name_tag:
            name, version = name_tag.split(":")
            return namespace, name, version

        return namespace, name_tag, "latest"
    raise RegistryPathError(f"Error in: '{schema_path}'")


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


def convert_date_string_to_date(date_string: str) -> datetime.datetime:
    """
    Convert string into datetime format

    :param date_string: date string in format [YYYY/MM/DD]. e.g. 2022/02/22
    :return: datetime format
    """
    return datetime.datetime.strptime(date_string, "%Y/%m/%d") + datetime.timedelta(days=1)


def order_samples(results: dict) -> List[dict]:
    """
    Order samples by their parent_guid

    # TODO: To make this function more efficient, we should write it in Rust!

    :param results: dictionary of samples. Structure: {
                            "sample": sample_dict,
                            "guid": sample.guid,
                            "parent_guid": sample.parent_guid,
                        }

    :return: ordered list of samples
    """
    # Find the Root Node
    # Create a lookup dictionary for nodes by their GUIDs
    guid_lookup = {entry["guid"]: entry for entry in results.values()}

    # Create a dictionary to map each GUID to its child GUID
    parent_to_child = {
        entry["parent_guid"]: entry["guid"]
        for entry in results.values()
        if entry["parent_guid"] is not None
    }

    # Find the root node
    root = None
    for guid, entry in results.items():
        if entry["parent_guid"] is None:
            root = entry
            break

    if root is None:
        raise ValueError("No root node found")

    ordered_sequence = []
    current = root

    while current is not None:
        ordered_sequence.append(current)
        current_guid = current["guid"]
        if current_guid in parent_to_child:
            current = guid_lookup[parent_to_child[current_guid]]
        else:
            current = None
    return ordered_sequence


def generate_guid() -> str:
    return str(uuid.uuid4())
