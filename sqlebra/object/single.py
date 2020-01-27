from .object import Object
from .. import exceptions as ex
from .. import variables as var


class Single(Object):

    # The following variables are defined by the inheriting class.
    col_val = False  # Name of the column containing the value (int_val, txt_val, etc)
    pyclass = False  # Python class

    @property
    def py(self):
        return self.__getattribute__(self.col_val)

    @py.setter
    def py(self, x):
        if not isinstance(x, self.pyclass):
            raise TypeError('SQLobject {} expects a value of type {}. Given type {} instead'.format(
                type(self), self.pyclass, type(x)))
        # Direct interfacing with the SQL database to maximize speed
        self.db.update(set={self.col_val: x},
                       where={'id': self.id, 'name': self.name, 'key': self.key, 'ind': self.ind})

    def delete(self):
        self.db.delete(where={'id': self.id, 'name': self.name, 'key': self.key, 'ind': self.ind})

    @classmethod
    def value2row(cls, x):
        """
        :param x: Python value to be converted
        :return: (dict) With (key, value) = (column, value) corresponding to a row in the database
            containing the specified value.
        """
        return {'class': cls.pyclass.__name__, cls.col_val: x}

    @classmethod
    def row2value(cls, row):
        """
        Extracts python variable from given row

        :param row: (tuple) A row from SQL database table [values]
        :return: Python value in the row
        """
        return row[var.COL_DICT[cls.col_val]]
