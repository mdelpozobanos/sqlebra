from .object_ import object_
from .. import variables as var
from .. import exceptions as ex


class Single(object_):

    # The following variables are defined by the inheriting class.
    col_val = False  # Name of the column containing the value (int_val, txt_val, etc)

    @property
    def py(self):
        return self.__getattribute__(self.col_val)

    @py.setter
    def py(self, x):
        """
        A single object can be assigned in many ways:

        If x is of type Object:
        1. If the Object is of the same class as self and is stored in the same SQL database, point self to the
            passed SQL-object. If self was the only reference to the previous SQL-object, delete it.
        2. Otherwise, extract the actual value from Object (i.e. Object.py) and continue with the python variable (
            continue reading).

        If x is a python variable:
        1. If self is the only reference to the current SQL-object, overwrite its value. This is equivalent to
            recycling the SQL-object's id.
        2. Otherwise, create a new SQL-object with the new value and point self (and its reference) to it.
        """
        if isinstance(x, object_):  # Assigning an SQL object...
            if x.pyclass != self.pyclass:  # ... of the same class
                raise ex.TypeError("Can't assign {} to {} object".format(x.__class__.__name__, self.sqlclass))
            if self.db == x.db:  # ... on the same SQL database
                # If this is the only reference to the current SQL object, delete it
                self.delete(del_ref=False)
                self.id = x.id
                return
            else:  # ... of a different class or on a different SQL database
                x = x.py

        # Check type
        if not isinstance(x, self.pyclass):
            raise ex.TypeError("Can't assign {} to {} object".format(x.__class__.__name__, self.sqlclass))

        # If this was the object's only reference (i.e. num_ref==1), update the value (easiest route)
        if self.num_ref == 1:
            self.db.update(self.db.tab_objs, set={self.col_val: x}, where={'id': self.id})
        else:  # 2. Attach reference to a new id containing the new value
            self.id = self.db.free_id()[0]
            self.db[self.id] = x

    @classmethod
    def value2row(cls, x):
        """
        :param x: Python value to be converted
        :return: (dict) With (key, value) = (column, value) corresponding to a row in the database
            containing the specified value.
        """
        return {'type': cls.pyclass.__name__, cls.col_val: x}

    @classmethod
    def row2value(cls, row):
        """
        Extracts python variable from given row

        :param row: (tuple) A row from SQL database table [values]
        :return: Python value in the row
        """
        return row[var.COL_DICT[cls.col_val]]

    def __bool__(self):
        py = self.py
        return bool(py)
        if py:
            return py
        else:
            return False
