import builtins
from ..object.item import Item
from ..object.nested import NestedIterator, NestedView
from .tuple_list_ import tuple_list_
from ..utils import argsort
from .. import exceptions as ex


class listView(NestedView):
    """View to a subset of an SQLebra list."""

    @property
    def py(self):
        return NestedView.py.fget(self)
        # return super(tuple_slice, self).py

    @py.setter
    def py(self, value):
        self.x[self.ind] = value

    def __setitem__(self, key, value):
        self.x[self.ind[key]] = value

    def __delitem__(self, key):
        del self.x[self.ind[key]]


class listIterator(NestedIterator):
    pass


class list_(tuple_list_):
    """SQL object of type list"""

    pyclass = builtins.list
    col_item = 'ind'
    _slice_class = listView
    _iterator_class = listIterator

    def __setitem__(self, item, value):
        # Retrieve selected item
        row = self[item]
        # Attach row to object
        if isinstance(row, self._slice_class):  # Allow slice assignment
            row.ref = self._slice_class(self, ind=item)
        elif item >= 0:  # Single item with positive index
            row.ref = Item(self, ind=item)
        elif item < 0:  # Single item with negative index
            row.ref = Item(self, ind=len(self) + item)
        # Update row value
        try:
            row.py = value
        except ex.TypeError:  # Delete row and add new variable
            row.delete(del_ref=False)
            id = [self.db.free_id()[0]]
            self.db[id] = [value]
            self.db.update(self.db.tab_items,
                           set={'child_id': id[0]},
                           where={'id': self.id,
                                  'ind': item})

    def __delitem__(self, key):
        ind = self._check_item_(key)
        # Retrieve and delete items one by one
        for i in ind[::-1]:
            x = self[i]
            x.ref = Item(self, ind=i)
            x.delete()

    # list only methods ----------------------------------------------------------------------

    def append(self, x):
        """Add an item to the end of the list. Equivalent to a[len(a):] = [x]."""
        self._append_(x)

    def extend(self, iterable):
        """Extend the list by appending all the items from the iterable. Equivalent to a[len(a):] = iterable."""
        if isinstance(iterable, list) or isinstance(iterable, tuple) or isinstance(iterable, tuple_list_):
            self.append(iterable)
        else:
            self.append([x for x in iterable])

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
            self.db.update(self.db.tab_items, set={'*': 'ind = ind + 1'},
                           where={'id': self.id, '*': 'ind >= {}'.format(i)})
        # Insert the new value in the selected position
        self.db[{'id': self.id, self.col_item: i}] = x

    def remove(self, x):
        """
        Remove the first item from the list whose value is equal to x. It raises a ValueError if there is no
        such item.

        :return:
        """
        # Find index of object
        ind = self.index(x)
        # Remove object
        x = self[ind]
        x.ref = Item(self, ind=ind)
        x.delete()

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
        x.ref = Item(self, ind=i)
        x_py = x.py
        # Delete variable and value from list
        x.delete()
        return x_py

    def reverse(self):
        """Reverse the elements of the list in place."""
        self.db.update(self.db.tab_items, set={'*': 'ind = {} - ind'.format(len(self))}, where={'id': self.id})

    def sort(self, key=None, reverse=False):
        """
        Sort the items of the list in place (the arguments can be used for sort customization, see sorted() for their
        explanation).

        :param key:
        :param reverse:
        :return:
        """
        # Find new indices
        ind = argsort(self.py)
        # Create sorted indices in key
        self.db.update(self.db.tab_items, set={'key': tuple(range(len(ind)))}, where={'id': (self.id,) * len(ind),
                                                                                      'ind': tuple(ind)})
        # Copy key to ind and empty key
        self.db.update(self.db.tab_items, set={'*': 'ind = key', 'key': None}, where={'id': self.id})

    def copy(self):
        """
        Return a shallow copy of the list. Equivalent to a[:].
        :return:
        """
        raise TypeError("""Forbidden operation. Do "self.db['copy_name'] = self" instead.""")
