from ..object.single import Single
import pickle
import types

raise NotImplemented


def fun(*args, **kwargs):
    """Does nothing"""
    pass


class Pickler(Single):
    """Parent class inherited by dtype that need pickling"""

    col_val = 'txt_val'
    pyclass = None

    def __init__(self, *args, **kwargs):
        super(pickle, self).__init__(*args, **kwargs)

    @property
    def x(self):
        # Retrieve string
        x_ser = pickle.x.fget(self)
        # Return deserialized function if not None
        if x_ser is None:
            return None
        else:
            return pickle.loads(x_ser)

    @x.setter
    def x(self, x):
        if self.pyclass:
            if not isinstance(x, self.pyclass):
                raise TypeError('SQLobject {} expects a value of type {}. Given type {} instead'.format(
                    type(self), self.pyclass, type(x)))
        else:
            self.pyclass = x.__class__.__name__
        # Serialize
        x_ser = pickle.dumps(x)
        # Direct interfacing with the SQL database to maximize speed
        self.db.update('values', {self.col_val: x_ser}, {'id': self.id})

    @staticmethod
    def row2value(row):
        """
        Extracts python variable from given row

        :param row: (tuple) A row from SQL database table [values]
        :return: Python value in the row
        """
        return pickle.loads(row.txt_val)