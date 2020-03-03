from ..object.nested import Nested, NestedIterator
from ..object.item import Item
import builtins
from sqlebra import exceptions as ex


class dict_iterator(NestedIterator):

    def __next__(self):
        self.current += 1
        if self.current < self.high:
            return self.obj.db.select(self.obj.db.tab_items, column=('key',),
                                      where={'id': self.obj.id, 'rn': self.current})[0][0]
        self.current = None
        raise StopIteration


class dict_(Nested):
    """SQL object of type dict"""

    pyclass = builtins.dict
    col_item = 'key'
    _iterator_class = dict_iterator

    @property
    def py(self):
        p = self.p
        if len(p) > 0:
            return {k: v.py for k, v in p.items()}
        else:
            return p

    @py.setter
    def py(self, x):
        if not isinstance(x, self.pyclass):
            raise TypeError('{} expects a value of type {}. Found type {} instead'.format(
                type(self), self.pyclass, type(x)))
        # Empty current
        self.clear()
        # Insert new objects
        len_x = len(x)
        ids = list(self.db.free_id(len_x))
        self.db[ids] = tuple(x.values())
        # Insert new items
        insert_items = {'id': (self.id,) * len_x,
                        self.col_item: tuple(x.keys()),
                        'child_id': tuple(ids)}
        self.db.insert(self.db.tab_items, insert_items)

    @property
    def p(self):
        """Half way through py property. List of SQLebra objects"""
        rows = self.db.select(self.db.tab_items, column=('child_id', ), where={'id': self.id}, order=('key',))
        if len(rows) > 0:
            objs = self.db[tuple(r[0] for r in rows)]
            return {k: v for k, v in zip(self.keys(), objs)}
        else:
            return self.pyclass()

    def __getitem__(self, item):
        return super(dict_, self).__getitem__(item)

    def __setitem__(self, key, value):
        # Retrieve selected item
        try:
            row = self[key]
        except KeyError:  # Add new key
            self.db[{'id': self.id, 'key': key}] = value
            row = self[key]
        # Attach row to object
        row.ref = Item(self, key=key)
        # Update row value
        try:
            row.py = value
        except ex.TypeError:  # Delete row and add new object
            row.delete(del_ref=False)
            id = [self.db.free_id()[0]]
            self.db[id] = [value]
            self.db.update(self.db.tab_items,
                           set={'child_id': id[0]},
                           where={'id': self.id,
                                  'key': key})

    def _check_item_(self, item):
        if item in self:
            return item
        else:
            raise KeyError("Key '{}' not found.".format(item))

    def __contains__(self, item):
        if not isinstance(item, str):
            try:
                item = str(item)
            except:
                raise TypeError("'{}' only supports 'str' keys. Found '{}' instead".format(type(self), type(item)))
        n = self.db.select(self.db.tab_items, column=('count(*)',), where={'id': self.id, self.col_item: item})[0][0]
        if n == 0:
            return False
        elif n == 1:
            return True
        else:
            return ex.CorruptedDatabase("Multiple values found for key '{}' of object {}".format(item, self.sql_info))

    def __delitem__(self, key):
        key = self._check_item_(key)
        item = self[key]
        item.ref = Item(self, key=key)
        item.delete()

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
            return self.x.db.select(self.x.db.tab_items,
                                    column=(self.x.col_item, ),
                                    where={'id': self.x.id, 'rn': self.current},
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
            id = self.x.db.select(self.x.db.tab_items,
                                  column=('child_id',),
                                  where={'id': self.x.id, 'rn': self.current},
                                  order=(self.x.col_item,))[0][0]
            return self.x.db[id]
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
