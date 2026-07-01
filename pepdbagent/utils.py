import datetime
import json
import uuid
from collections.abc import Iterable
from hashlib import md5

import ubiquerg
from peprs.const import SAMPLE_RAW_DICT_KEY

from pepdbagent.exceptions import RegistryPathError


def is_valid_registry_path(rpath: str) -> bool:
    """Verify that a registry path is valid.

    Checks that the path contains a forward slash dividing two non-empty strings.

    Args:
        rpath: Registry path to test.

    Returns:
        True if the path is a valid registry path.
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
    """Check if every element of an iterable is a str.

    Args:
        iterable: An iterable item.

    Returns:
        True if every element is a string.
    """
    if not isinstance(iterable, Iterable):
        return False
    return all([isinstance(item, str) for item in iterable])


def create_digest(project_dict: dict) -> str:
    """Create an MD5 digest for a PEP project.

    Args:
        project_dict: Project dictionary.

    Returns:
        MD5 hex digest of the sample table.
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


def registry_path_converter(registry_path: str) -> tuple[str, str, str]:
    """Convert a registry path to (namespace, name, tag).

    Args:
        registry_path: Registry path with structure "namespace/name:tag".

    Returns:
        Tuple of (namespace, name, tag).

    Raises:
        RegistryPathError: If the path is not a valid registry path.
    """
    if is_valid_registry_path(registry_path):
        reg = ubiquerg.parse_registry_path(registry_path)
        namespace = reg["namespace"]
        name = reg["item"]
        tag = reg["tag"]
        return namespace, name, tag

    raise RegistryPathError(f"Error in: '{registry_path}'")


def schema_path_converter(schema_path: str) -> tuple[str, str, str]:
    """Convert a schema path to (namespace, name, version).

    Args:
        schema_path: Schema path with structure "namespace/name:version".

    Returns:
        Tuple of (namespace, name, version).

    Raises:
        RegistryPathError: If the path is invalid.
    """
    if "/" in schema_path:
        namespace, name_tag = schema_path.split("/")
        if ":" in name_tag:
            name, version = name_tag.split(":")
            return namespace, name, version

        return namespace, name_tag, "latest"
    raise RegistryPathError(f"Error in: '{schema_path}'")


def tuple_converter(value: tuple | list | str | None) -> tuple:
    """Convert a string, list, or tuple to a tuple.

    Args:
        value: Value to convert.

    Returns:
        Tuple of strings.
    """
    if isinstance(value, str):
        value = [value]
    if value:
        return tuple(value)
    return tuple(
        " ",
    )


def convert_date_string_to_date(date_string: str) -> datetime.datetime:
    """Convert a date string to a datetime.

    Args:
        date_string: Date string in format YYYY/MM/DD, e.g., "2022/02/22".

    Returns:
        Parsed datetime offset by one day.
    """
    return datetime.datetime.strptime(date_string, "%Y/%m/%d") + datetime.timedelta(
        days=1
    )


def order_samples(results: dict) -> list[dict]:
    """Order samples by their parent_guid chain.

    # TODO: To make this function more efficient, we should write it in Rust!

    Args:
        results: Dict of samples keyed by guid, each value a dict with
            "sample", "guid", and "parent_guid" keys.

    Returns:
        Ordered list of samples from root to leaf.
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
