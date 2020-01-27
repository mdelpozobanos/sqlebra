from ..object.nested import Nested
import builtins
from sqlebra import exceptions as ex


class dict_(Nested):
    """SQL object of type dict"""

    pyclass = builtins.dict
    col_item = 'key'

    @property
    def py(self):
        x = {}
        try:
            rows = self.db[{'id': self.id, '*': 'root = 0'}]
        except ex.VariableError:  # Empty list
            # Possibly an empty list. Retrieve root to assert
            self.db[{'id': self.id, '*': 'root = 1'}]
        else:
            for key, _, val in zip(*rows):
                x[key] = val.py
        return x

    @py.setter
    def py(self, x):
        if not isinstance(x, self.pyclass):
            raise TypeError('{} expects a value of type {}. Found type {} instead'.format(
                type(self), self.pyclass, type(x)))
        # Empty current
        self.clear()
        # Insert new values
        value_item = {'id': self.id, 'root': False, 'user_defined': False}
        for key, value in x.items():
            value_item['name'] = self._nameitem_(key)
            value_item[self.col_item] = key
            self.db[value_item.copy()] = value

    def __getitem__(self, item):
        x = super(dict_, self).__getitem__(self._check_item_(item))[2]
        if isinstance(item, str):
            return x[0]
        else:
            return x

    def __setitem__(self, key, value):
        try:
            item = self._check_item_(key)
        except KeyError:  # Add new key
            self.db[{'id': self.id,
                     'name': self._nameitem_(key),
                     'root': False,
                     'user_defined': False,
                     self.col_item: key}] = value
        else:
            # Retrieve and update row
            row = self.db[{'id': self.id, self.col_item: item}][2][0]
            try:
                row.py = value
            except TypeError:
                row.delete()
                self.db.delete(where={'id': self.id, self.col_item: key})
                self[key] = value

    def _check_item_(self, item):
        if item in self:
            return item
        else:
            raise KeyError("Key '{}' not found.".format(item))

    def __contains__(self, item):
        if not isinstance(item, str):
            raise TypeError("'{}' only supports 'str' keys. Found '{}' instead".format(type(self), type(item)))
        n = self.db.select(column=('count(*)',), where={'id': self.id, self.col_item: item})[0][0]
        if n == 0:
            return False
        elif n == 1:
            return True
        else:
            return ex.CorruptedDatabase("Multiple values found for key '{}' of object {}".format(item, self.sql_info))

    def __iter__(self):
        return self.keys()

    def keys(self):
        return dict_keys(self)

    def values(self):
        return dict_values(self)

    def items(self):
        return dict_items(self)

    def pop(self, key):
        try:
            item = self._check_item_(key)
        except KeyError:
            return None
        else:
            # Retrieve row
            row = self.db[{'id': self.id, self.col_item: item}][2][0]
            # Retrieve value and delete key
            row_py = row.py
            row.delete()
            self.db.delete(where={'id': self.id, self.col_item: key})


class dict_keys:
    """View of dict keys"""

    def __init__(self, x):
        self.x = x
        self.current = -1
        self.high = len(self.x)

    def __iter__(self):
        self.current = -1
        return self

    def __next__(self):
        self.current += 1
        if self.current < self.high:
            return self.x.db.select(column=(self.x.col_item, ),
                                    where={'id': self.x.id, 'rn': self.current, 'root': 0},
                                    order=(self.x.col_item, ))[0][0]
        raise StopIteration


class dict_values:
    """View of dict values"""

    def __init__(self, x):
        self.x = x
        self.current = -1
        self.high = len(self.x)

    def __iter__(self):
        self.current = -1
        return self

    def __next__(self):
        self.current += 1
        if self.current < self.high:
            return self.x.db[{'id': self.x.id, 'rn': self.current, 'root': 0}][2][0]
        raise StopIteration


class dict_items:
    """View of dict items"""

    def __init__(self, x):
        self.x = x
        self.current = -1
        self.high = len(self.x)

    def __iter__(self):
        self.current = -1
        return self

    def __next__(self):
        self.current += 1
        if self.current < self.high:
            item = self.x.db[{'id': self.x.id, 'rn': self.current, 'root': 0}]
            return item[0][0], item[2][0]
        raise StopIteration
