from ..object.nested import Nested
import builtins
from .. import py2sql
from sqlebra import exceptions as ex


class list_(Nested):
    """SQL object of type list"""

    pyclass = builtins.list
    col_item = 'ind'

    @property
    def py(self):
        x = []
        try:
            rows = self.db[{'id': self.id, '*': 'root = 0'}][2]
        except ex.VariableError:  # Empty list
            # Possibly an empty list. Retrieve root to assert
            self.db[{'id': self.id, '*': 'root = 1'}]
        else:
            for row in rows:
                x.append(row.py)
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
        for ind, value in enumerate(x):
            value_item['name'] = self._nameitem_(ind)
            value_item[self.col_item] = ind
            self.db[value_item.copy()] = value

    def __getitem__(self, item):
        x = super(list_, self).__getitem__(self._check_item_(item))[2]
        if isinstance(item, int):
            return x[0]
        else:
            return x

    def __setitem__(self, key, value):
        item = self._check_item_(key)
        # Retrieve selected row
        row = self.db[{'id': self.id, self.col_item: item}][2]
        if len(row) != 1:
            if len(row) == 0:
                raise ex.CorruptedDatabase('Item {} not found in the SQL database'.format(key))
            else:
                raise ex.ValueError('Multiple rows selected for setting')
        # Retrieve row
        row = self.db[{'id': self.id, self.col_item: item[0]}][2][0]
        try:  # Update row
            row.py = value
        except TypeError:  # Delete row and add new variable
            row.delete()
            self.db.delete(where={'id': self.id, self.col_item: key})
            self.db[{'id': self.id,
                     'root': False,
                     'user_defined': False,
                     'name': self._nameitem_(item[0]),
                     self.col_item: item[0]
                     }] = value

    def _check_item_(self, item):
        if isinstance(item, int):
            item = [item]
        elif isinstance(item, list_):
            pass
        else:
            raise TypeError("Index must be of type 'int' or 'list'. Found '{}' instead".format(type(item)))
        len_x = len(self)
        for i in range(len(item)):
            if item[i] < 0:
                item[i] = len_x + item[i]
            if item[i] >= len_x:
                raise ValueError("Index out of range")
        return item

    def append(self, x):
        """Add an item to the end of the list. Equivalent to a[len(a):] = [x]."""
        ind = len(self)
        self.db[{'id': self.id,
                 'name': self._nameitem_(ind),
                 self.col_item: ind,
                 'root': False,
                 'user_defined': False}] = x

    def extend(self, iterable):
        """Extend the list by appending all the items from the iterable. Equivalent to a[len(a):] = iterable."""
        for x in iterable:
            self.append(x)

    def insert(self, i, x):
        """
        Insert an item at a given position. The first argument is the index of the element before which to insert,
        so a.insert(0, x) inserts at the front of the list, and a.insert(len(a), x) is equivalent to a.append(x)

        :param i:
        :param x:
        :return:
        """
        if i < len(self):  # Need to update the index of all elements to the right of i
            # Interface directly with the database to maximize speed
            self.db.update(set={'*': 'ind = ind + 1'}, where={'id': self.id, '*': 'ind >= {}'.format(i)})
            # self.db.execute('update {} set ind = ind + 1 where id = ? and ind >= ?'.format(self.db.name), (self.id, i))
        # Insert the new value in the selected position
        self.db[{'id': self.id,
                 'name': self.name,
                 self.col_item: i,
                 'root': False,
                 'user_defined': False}] = x

    def index(self, x, start=None, end=None):
        """
        Return zero-based index in the list of the first item whose value is equal to x. Raises a ValueError if
        there is no such item.

        The optional arguments start and end are interpreted as in the slice notation and are used to limit the
        search to a particular subsequence of the list. The returned index is computed relative to the beginning of the
        full sequence rather than the start argument.

        :param x:
        :param start:
        :param end:
        :return:
        """
        if type(x) in py2sql.py2sql_single:
            py2sql.py2sql_single[type(x)].value2row(x)
            x_row = {'id': self.id, 'rn': 0, **py2sql.py2sql_single[type(x)].value2row(x)}
            # Search for matching class
            ind = self.db.select(column=('ind',), where=x_row, order=('ind', ))
            if len(ind) == 0:
                raise ValueError('Value {} not found in list'.format(x))
            else:
                return ind[0][0]

        else:
            raise NotImplemented
            # Search for matching class
            pos_ind = self.db.select(column=('ind', ),
                                     where={'id': self.id, 'class': x.__class__.__name__, '*': 'ind is not NULL'})
            # Search for matching values
            if len(pos_ind) == 0:
                raise ValueError('Value {} not found in list'.format(x))
            # Check value of possible matching indices
            pos_ind = [x[0] for x in pos_ind]
            for row in self.db.v[{'id': self.id, 'ind': pos_ind}]:
                if row.multivalue and row.x.x == x:
                    return row.ind
                elif row.x == x:
                    return row.ind
            # If this point is reached, no matching value was found
            raise ValueError('Value {} not found in list'.format(x))

    def remove(self, x):
        """
        Remove the first item from the list whose value is equal to x. It raises a ValueError if there is no
        such item.

        :return:
        """
        # Find index of object
        ind = self.index(x)
        # Remove object
        self.db[{'id': self.id, 'ind': ind}][2][0].delete()
        # Update indices if necessary
        if ind < len(self)-1:
            self.db.update(set={'*': 'ind = ind - 1'}, where={'id': self.id, '*': 'ind>{}'.format(ind)})

    def pop(self, i=-1):
        """
        Remove the item at the given position in the list, and return it. If no index is specified, a.pop() removes
        and returns the last item in the list. (The square brackets around the i in the method signature denote that
        the parameter is optional, not that you should type square brackets at that position. You will see this
        notation frequently in the Python Library Reference.)

        :param i: (int)
        :return:
        """
        if not isinstance(i, int):
            raise TypeError('i must be {}. Found {} instead'.format(int, type(i)))
        i = self._check_item_(i)[0]
        # Retrieve value
        x = self[i]
        x_py = x.py
        # Delete variable and value from list
        x.delete()
        self.db.delete(where={'id': self.id, 'ind': i})
        # Update indices if necessary
        if i < len(self):
            self.db.update(set={'*': 'ind = ind - 1'}, where={'id': self.id, '*': 'ind>{}'.format(i)})
        return x_py

    def count(self, x):
        """
        Return the number of times x appears in the list.

        :param x:
        :return:
        """
        if type(x) in py2sql.py2sql_single:
            py2sql.py2sql_single[type(x)].value2row(x)
            x_row = {'id': self.id, **py2sql.py2sql_single[type(x)].value2row(x)}
            # Search for matching class
            return self.db.select(column=('count(*)',), where=x_row, order=('ind', ))[0][0]
        else:
            raise NotImplemented

            # Search for matching class
            pos_ind = self.db.execute('select ind from [values] where id = ? and class_name = ?',
                                      (self.id, x.__class__.__name__))
            # Search for matching values
            if len(pos_ind) == 0:
                raise ValueError('Value {} not found in list'.format(x))
            # Check value of possible matching indices
            pos_ind = [x[0] for x in pos_ind]
            count = 0
            for row in self.db.v[{'id': self.id, 'ind': pos_ind}]:
                if row.multivalue and row.py.py == x:
                    count += 1
                elif row.py == x:
                    count += 1
            return count

    def sort(self, key=None, reverse=False):
        """
        Sort the items of the list in place (the arguments can be used for sort customization, see sorted() for their
        explanation).

        :param key:
        :param reverse:
        :return:
        """
        raise NotImplemented

    def copy(self):
        """
        Return a shallow copy of the list. Equivalent to a[:].
        :return:
        """
        raise NotImplemented
        return list_(self.db, id=self.id)

    def __iter__(self):
        return list_iterator(self)


class list_iterator:

    def __init__(self, x):
        self.x = x
        self.current = -1
        # Static parameter. A dynamic parameter would be more flexible but slower. Anyway, one shouldn't
        # change the length of an iterator on the go.
        self.high = len(self.x)

    def __next__(self):
        self.current += 1
        if self.current < self.high:
            return self.x[self.current]
        self.current = None
        return StopIteration
