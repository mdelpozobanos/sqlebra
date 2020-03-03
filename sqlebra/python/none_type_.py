from ..object.single import Single
from .. import exceptions as ex


class NoneType_(Single):
    """SQLebra object of type None"""

    pyclass = None.__class__
    col_val = None

    @property
    def py(self):
        return None

    @py.setter
    def py(self, x):
        raise ex.TypeError('{} cannot be set'.format(type(self)))

    @classmethod
    def value2row(cls, x):
        """
        Hypothetical row in table [values] within the SQL database containing the specified value.

        :param x: (bool) Value to be specified
        :return: (dict) With (key, value) = (column, value) corresponding to a row in table [values] within the SQL
        database containing the specified value.
        """
        return {'type': NoneType_.pyclass.__name__}

    @classmethod
    def row2value(cls, row):
        """
        Extracts python variable from given row

        :param row: (tuple) A row from SQL database table [values]
        :return: Python value in the row
        """
        return None
