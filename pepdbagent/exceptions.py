"""Custom error types"""


class PEPDatabaseAgentError(Exception):
    """Base error type for pepdbagent custom errors."""

    def __init__(self, msg):
        super(PEPDatabaseAgentError, self).__init__(msg)


class SchemaError(PEPDatabaseAgentError):
    def __init__(self):
        super().__init__("""PEP_db connection error! The schema of connected db is incorrect""")


class RegistryPathError(PEPDatabaseAgentError):
    def __init__(self, msg=""):
        super().__init__(f"""Provided registry path is incorrect. {msg}""")


class ProjectNotFoundError(PEPDatabaseAgentError):
    def __init__(self, msg=""):
        super().__init__(f"""Project does not exist. {msg}""")


class ProjectUniqueNameError(PEPDatabaseAgentError):
    def __init__(self, msg=""):
        super().__init__(f"""{msg}""")


class IncorrectDateFormat(PEPDatabaseAgentError):
    def __init__(self, msg=""):
        super().__init__(f"""Incorrect date format was provided. {msg}""")


class FilterError(PEPDatabaseAgentError):
    def __init__(self, msg=""):
        super().__init__(f"""pepdbagent filter error. {msg}""")


class ProjectNotInFavorites(PEPDatabaseAgentError):
    """
    Project doesn't exist in favorites
    """

    def __init__(self, msg=""):
        super().__init__(f"""Project is not in favorites list. {msg}""")


class ProjectAlreadyInFavorites(PEPDatabaseAgentError):
    """
    Project doesn't exist in favorites
    """

    def __init__(self, msg=""):
        super().__init__(f"""Project is already in favorites list. {msg}""")


class SampleNotFoundError(PEPDatabaseAgentError):
    def __init__(self, msg=""):
        super().__init__(f"""Sample does not exist. {msg}""")


class SampleTableUpdateError(PEPDatabaseAgentError):
    def __init__(self, msg=""):
        super().__init__(f"""Sample table update error. {msg}""")


class ProjectDuplicatedSampleGUIDsError(SampleTableUpdateError):
    def __init__(self, msg=""):
        super().__init__(f"""Project has duplicated sample GUIDs. {msg}""")


class SampleAlreadyExistsError(PEPDatabaseAgentError):
    def __init__(self, msg=""):
        super().__init__(f"""Sample already exists. {msg}""")


class ViewNotFoundError(PEPDatabaseAgentError):
    def __init__(self, msg=""):
        super().__init__(f"""View does not exist. {msg}""")


class SampleNotInViewError(PEPDatabaseAgentError):
    def __init__(self, msg=""):
        super().__init__(f"""Sample is not in the view. {msg}""")


class SampleAlreadyInView(PEPDatabaseAgentError):
    """
    Sample is already in the view exception
    """

    def __init__(self, msg=""):
        super().__init__(f"""Sample is already in the view. {msg}""")


class ViewAlreadyExistsError(PEPDatabaseAgentError):
    """
    View is already in the project exception
    """

    def __init__(self, msg=""):
        super().__init__(f"""View already in the project. {msg}""")


class NamespaceNotFoundError(PEPDatabaseAgentError):
    def __init__(self, msg=""):
        super().__init__(f"""Project does not exist. {msg}""")


class HistoryNotFoundError(PEPDatabaseAgentError):
    def __init__(self, msg=""):
        super().__init__(f"""History does not exist. {msg}""")


class UserNotFoundError(PEPDatabaseAgentError):
    def __init__(self, msg=""):
        super().__init__(f"""User does not exist. {msg}""")


class SchemaDoesNotExistError(PEPDatabaseAgentError):
    def __init__(self, msg=""):
        super().__init__(f"""Schema does not exist. {msg}""")


class SchemaAlreadyExistsError(PEPDatabaseAgentError):
    def __init__(self, msg=""):
        super().__init__(f"""Schema already exists. {msg}""")


class SchemaVersionDoesNotExistError(PEPDatabaseAgentError):
    def __init__(self, msg=""):
        super().__init__(f"""Schema version does not exist. {msg}""")


class SchemaVersionAlreadyExistsError(PEPDatabaseAgentError):
    def __init__(self, msg=""):
        super().__init__(f"""Schema version already exists. {msg}""")


class SchemaTagDoesNotExistError(PEPDatabaseAgentError):
    def __init__(self, msg=""):
        super().__init__(f"""Schema tag does not exist. {msg}""")


class SchemaTagAlreadyExistsError(PEPDatabaseAgentError):
    def __init__(self, msg=""):
        super().__init__(f"""Schema tag already exists. {msg}""")
