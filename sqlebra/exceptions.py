# TODO: Review exceptions once SQLebra is completed


class SQLebraError(Exception):
    pass


class EmptyTableError(SQLebraError):
    pass


class ValueError(SQLebraError):
    pass


class VariableError(SQLebraError):
    pass


class ObjectError(SQLebraError):
    pass


class ItemError(SQLebraError):
    pass


class CorruptedDatabase(SQLebraError):
    """The database has an unexpected structure"""
    pass


class ConnectionError(SQLebraError):
    pass


class TypeError(SQLebraError):
    pass


class SQLModuleNotFoundError(SQLebraError):
    pass
