from collections.abc import Iterable


def is_valid_resgistry_path(rpath: str) -> bool:
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
