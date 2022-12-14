""" Custom error types """


__all__ = ["SchemaError"]


class SchemaError(Exception):
    def __init__(self):
        # Call the base class constructor with the parameters it needs
        super().__init__(
            """PEP_db connection error! The schema of connected db is incorrect"""
        )
