""" Custom error types """


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
