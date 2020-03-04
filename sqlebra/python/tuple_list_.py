from sqlebra.object.nested import Nested
from sqlebra.object.object_ import object_


class tuple_list_(Nested):
    """
    This class is used as a common ground for list_ and tuple_

    Why not make, e.g., list_ inherit from tuple_? Because this will hinder the differentiation between the two,
    i.e., using "isinstance".
    """

    _slice_class = None
    _iterator_class = None

    @property
    def py(self):
        p = self.p
        if len(p) > 0:
            return self.pyclass(p_i.py for p_i in p)
        else:
            return p

    @py.setter
    def py(self, x):
        if not isinstance(x, self.pyclass):
            raise TypeError('{} expects a value of type {}. Found type {} instead'.format(
                self.sqlclass, self.pyclass, type(x)))
        # Empty current
        self.clear()
        if len(x) == 0:
            return
        # Insert new objects
        len_x = len(x)
        ids = list(self.db.free_id(len_x))
        self.db[ids] = x
        # Insert list items
        insert_items = {'id': (self.id,) * len_x,
                        self.col_item: tuple(range(len_x)),
                        'child_id': tuple(ids)}
        self.db.insert(self.db.tab_items, insert_items)

    @property
    def p(self):
        """Half way through py property. List of SQLebra objects"""
        rows = self.db.select(self.db.tab_items, column=('child_id',), where={'id': self.id}, order=('ind',))
        if len(rows) > 0:
            return self.db[self.pyclass(r[0] for r in rows)]
        else:
            return self.pyclass()

    def __getitem__(self, item):
        """
        if item is int:
            Return object in position item
        if item is list:
            Return an SQLebra list object with the slice
        """
        if isinstance(item, slice) or isinstance(item, list):  # Return a list slice
            item = self._check_item_(item)
            return self._slice_class(self, item)
        # else: Return a single object
        return super(tuple_list_, self).__getitem__(item)

    def __setitem__(self, key, value):
        raise NotImplemented

    def __delitem__(self, key):
        raise NotImplemented

    def __iter__(self):
        return self._iterator_class(self)

    def _check_item_(self, item):
        len_x = len(self)
        if isinstance(item, int):
            item = [item]
        elif isinstance(item, slice):
            item = [i for i in range(*item.indices(len_x))]
        elif not isinstance(item, list):
            raise TypeError("Index must be of type 'int', 'slice' or 'list'. Found '{}' instead".format(type(item)))
        for i in range(len(item)):
            try:
                if item[i] < 0:
                    item[i] = len_x + item[i]
            except e:
                if not isinstance(item[i], int):
                    raise TypeError("Indices values must be of type 'int'. Found '{}' instead".format(type(item[i])))
            if item[i] >= len_x:
                raise ValueError("Index out of range")
        return item

    # tuple & list operators ----------------------------------------------------------------------

    def __add__(self, other):
        if isinstance(other, object_):
            return self.py + other.py
        else:
            return self.py + other

    def __iadd__(self, other):
        return self._extend_(other)

    # tuple & list methods ----------------------------------------------------------------------

    def _extend_(self, x):
        """Append is not defined for tuple, but its functionality is used by tuple."""
        # Retrieve ids of appended objects
        if isinstance(x, object_):
            if x.db == self.db:  # Same database, use existing object ids
                ids = x.child_id
                # Insert list items
                len_x = len(ids)
            else:  # Different database, add new objects
                x = x.py
        # Add new objects to the database and retrieve ids
        if not isinstance(x, object_):
            if not hasattr(x, '__iter__'):
                x = (x,)
            # Insert new objects
            len_x = len(x)
            ids = list(self.db.free_id(len_x))
            self.db[ids] = x

        # Insert list items
        ind0 = len(self)
        insert_items = {'id': (self.id,) * len_x,
                        self.col_item: tuple(range(ind0, ind0+len_x)),
                        'child_id': tuple(ids)}
        self.db.insert(self.db.tab_items, insert_items)
        return self

    def count(self, x):
        """
        Return the number of times x appears in the list.

        :param x:
        :return:
        """
        if isinstance(x, object_):
            # If the object is in the same database, find items pointing to it (i.e. child_id == x.id)
            if x.db == self.db:  # Find items pointing to x.id
                return self.db.select(self.db.tab_items, column=('count(*)',),
                                      where={'id': self.id, 'child_id': x.id})[0][0]
            # else:  # Different database. Extract value and count
            x = x.py
        # Compare with external value
        return self.py.count(x)

    def index(self, x, *args):
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
        if isinstance(x, object_):
            # If the object is in the same database, find items pointing to it (i.e. child_id == x.id)
            if x.db == self.db:  # Find items pointing to x.id
                if len(args) == 0:
                    where = {}
                elif len(args) == 1:
                    where = {'*': 'ind >= {}'.format(args[0])}
                elif len(args) == 2:
                    where = {'*': 'ind between {} and {}'.format(args[0], args[1])}
                ind = self.db.select(self.db.tab_items, column=('ind',),
                                     where={'id': self.id, 'child_id': x.id, **where})
                if len(ind) == 0:  # Try with the value
                    x = x.py
                else:
                    return ind[0][0]
            else:  # Different database. Extract value and count
                x = x.py
        # Compare with external value
        try:
            return self.py.index(x, *args)
        except ValueError:
            raise ValueError('{} {} is not in {}'.format(self.sql_info, x, self.sqlclass))


def method_generator(fcn):
    """Generates methods to be added to tuple_list_"""

    def method(self, *args, **kwargs):
        return fcn(self.py, *args, **kwargs)

    method.__name__ = fcn.__name__
    return method


# Add methods that are not supported natively
for method in (any, min, max, map, sorted, sum):
    cls_method = method_generator(method)
    setattr(tuple_list_, cls_method.__name__, cls_method)
