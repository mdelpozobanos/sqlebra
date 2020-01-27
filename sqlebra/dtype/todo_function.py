from sqlebra.dtype.todo_pickle import SQLpickle
import pickle
import types
raise NotImplemented


def fun(*args, **kwargs):
    """Does nothing"""
    pass


class SQLfunction(SQLpickle):
    """SQLobject of type function"""

    pyclass = types.FunctionType
    _default_x = pickle.dumps(fun)

    @staticmethod
    def value2row(x):
        """
        Hypothetical row in table [values] within the SQL database containing the specified value.

        :param x: (int) Value to be specified
        :return: (dict) With (key, value) = (column, value) corresponding to a row in table [values] within the SQL
        database containing the specified value.
        """
        return {'class_name': SQLfunction.pyclass.__name__, SQLfunction.col_val: pickle.dumps(x)}
