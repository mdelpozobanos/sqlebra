import builtins
from . import _Sequence__SequenceSlice_generator, Sequence
from sqlebra._utils import argsort

SequenceSlice = _Sequence__SequenceSlice_generator(list)


class listSlice(SequenceSlice):

    def __init__(self, ref, item, id):
        """

        Parameters
        ----------
        ref: `sqlebra.object`
            SQLebra object owning the item.
        item: slice
            Selected items
        id: tuple
            List of object ids for each item
        """
        super().__init__(ref, item, id)
        super(SequenceSlice, self).__init__(
            tuple(
                ref.db._objs.obj[ref._Multi__item_class(ref, ind=item_i), id_i]
                for item_i, id_i in zip(range(item.start, item.stop, item.step), id)
            )
        )


class list_(Sequence):
    """SQL object of type list"""

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # object_
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [object_] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [object_; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pyclass = builtins.list  # Python class

    # [object_; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [object_; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # Multi
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [object_; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pass

    # [object_; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [object_; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    _Multi__slice_class = listSlice

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # list_
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [list_] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [list_; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pass

    # [list_; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [list_; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # [list_] FD
    # ==================================================================================================================
    # Methods/properties fully defined here. They can be overloaded by inheriting classes

    # [list_; FD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    def __setitem__(self, item, value):
        row = self[item]
        row._ref.py = value

    def __delitem__(self, item):
        row = self[item]
        row._delete()

    def append(self, x):
        """Add an item to the end of the list. Equivalent to a[len(a):] = [x]."""
        self._Sequence__extend((x,))

    def extend(self, iterable):
        """Extend the list by appending all the items from the iterable. Equivalent to a[len(a):] = iterable."""
        if isinstance(iterable, (list, tuple, Sequence)):
            self._Sequence__extend(iterable)
        else:
            self._Sequence__extend(tuple(x for x in iterable))

    def insert(self, i, x):
        """
        Insert an item at a given position. The first argument is the index of the element before which to insert,
        so a.insert(0, x) inserts at the front of the list, and a.insert(len(a), x) is equivalent to a.append(x)

        :param i:
        :param x:
        :return:
        """
        i, _, _ = self._item2where(i)
        if i < len(self):  # Need to update the index of all elements to the right of i
            # Interface directly with the database to maximize speed
            self.db._items[{'id': self._id, '!@*': 'ind >= {}'.format(i)}] = {'ind': '!@*ind + 1'}
        # Insert the new value in the selected position
        self._draft_obj(self.db._draft, (x, ), self._id, i)
        self.db._draft._commit()

    def remove(self, x):
        """
        Remove the first item from the list whose value is equal to x. It raises a ValueError if there is no
        such item.

        :return:
        """
        # Find index of object
        ind = self.index(x)
        # Remove object
        self[ind]._delete()

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
        i, _, _ = self._item2where(i)
        # Retrieve value
        x = self[i]
        x_py = x.py
        # Delete variable and value from list
        x._delete()
        return x_py

    def reverse(self):
        """Reverse the elements of the list in place."""
        self.db._items[{'id': self._id}] = {'ind': '!@* {} - ind'.format(len(self))}

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
        self.db._update(self.db._name_tab_items, set={'key': tuple(range(len(ind)))}, where={'id': (self._id,) * len(ind),
                                                                                      'ind': tuple(ind)})
        # Copy key to ind and empty key
        self.db._update(self.db._name_tab_items, set={'!@*': 'ind = key', 'key': None}, where={'id': self._id})

    def copy(self):
        """
        Return a shallow copy of the list. Equivalent to a[:].
        :return:
        """
        raise TypeError("""Forbidden operation. Do "self.db['copy_name'] = self" instead.""")


    # [list_; FD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [list_; FD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass
