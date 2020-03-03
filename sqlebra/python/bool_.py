from ..object.single import Single
from ..object.numeric import Numeric
import builtins


class bool_(Single, Numeric):
    """SQLobject of type bool"""

    pyclass = builtins.bool
    col_val = 'bool_val'

    @classmethod
    def row2value(cls, row):
        """
        Extracts python variable from given row

        :param row: (tuple) A row from SQL database table [values]
        :return: Python value in the row
        """
        return cls.pyclass(super(bool_, cls).row2value(row))
