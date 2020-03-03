class VariableError(Exception):
    pass


class ObjectError(Exception):
    pass


class CorruptedDatabase(Exception):
    pass


class ConnectionError(ConnectionError):
    pass


class TypeError(TypeError):
    pass
