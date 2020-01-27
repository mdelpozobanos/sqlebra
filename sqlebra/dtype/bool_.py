from ..object.single import Single
import builtins


class bool_(Single):
    """SQLobject of type bool"""

    pyclass = builtins.bool
    col_val = 'bool_val'

    @property
    def py(self):
        return super(bool_, self).py == 1

    @py.setter
    def py(self, x):
        if not isinstance(x, self.pyclass):
            x = self.pyclass(x)
        self.db.update(set={self.col_val: x}, where={'id': self.id})

    @classmethod
    def row2value(cls, row):
        """
        Extracts python variable from given row

        :param row: (tuple) A row from SQL database table [values]
        :return: Python value in the row
        """
        return cls.pyclass(super(bool_, cls).row2value(row))
