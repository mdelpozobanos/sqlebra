from sqlebra.object.multi import MultiItem, Multi
from sqlebra.object.object_ import object_
from sqlebra import exceptions as ex


class SequenceItem(MultiItem):

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # MultiItem
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [MultiItem] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [MultiItem; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pass

    # [MultiItem; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    @property
    def _where(self):
        """Dictionary used as the `where` argument to select the item's row in table ITEMS"""
        return {'id': self._ref._id, 'ind': self._ind}

    # [MultiItem; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # [MultiItem] Overloaded methods
    # ==================================================================================================================
    # Methods/properties overloaded

    def _delete(self):
        super()._delete()
        self.db._items[{'id': self._ref._id, '!@*': 'ind > {}'.format(self._where['ind'])}] = {'ind': '!@*ind - 1'}


def _Sequence__SequenceSlice_generator(caller_pyclass):

    class SequenceSlice(caller_pyclass):
        """Base class for a sequence slice."""

        pyclass = caller_pyclass

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
            self.db = ref.db
            self._ref = ref

        @property
        def py(self):
            return self.pyclass(x.py for x in self)

        @py.setter
        def py(self, x):
            raise AttributeError('readonly attribute')

        @property
        def objs(self):
            return self._ref.pyclass(self)

        def _delete(self):
            for i in self[::-1]:
                i._delete()

    return SequenceSlice


class Sequence(Multi):
    """
    This class is used as a common ground for list_ and tuple_

    Why not make, e.g., list_ inherit from tuple_? Because this will hinder the differentiation between the two,
    i.e., using "isinstance".
    """

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # object_
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [object_] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [object_; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    # Python values
    # -------------

    @property
    def py(self):
        """Python value of the object"""
        objs = self.objs
        if len(objs) > 0:
            return self.pyclass(o.py for o in objs)
        else:
            return objs

    @py.setter
    def py(self, x):
        object_.py.fset(self, x)

    # [object_; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    # def _draft_obj(cls, db_draft, value):

    # [object_; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # multi
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [multi] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [multi; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    @property
    def objs(self):
        """Half-way-through py property. Object of type `self.pyclass` with values in SQLebra object form"""
        return self.pyclass(self._Multi__objs_tuple)

    # [multi; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    @classmethod
    def _draft_obj(cls, db_draft, value, _id=None, _ind_offset=0):
        """
        Draft a value's rows from tables OBJECTS and ITEMS representing the value in the database, and returns its
        object identifier.

        Parameters
        ----------
        db_draft: `sqlebra.database.database.DatabaseDraft'
            Draft object of calling database
        value: `cls.pyclass`
            Python value to be drafted
        _id: int
            Object identifier. If not specified (default) a new object will be drafted. If specified, only items will be
            drafted.
        _ind_offset: int
            Index offset of drafted items.

        Returns
        -------
        int:
            Unique object identifier pertaining to the drafted object. If the object is new, the identifier will be < 0.

        Notes
        -----
        Multi objects are defined by multiple rows across tables OBJECTS and ITEMS. The definition of this procedure is
        fully decentralised since multi classes may follow very difference drafting procedures.

        Multi objects are not directly hashable in SQLebra. If a multi object needs to be reused, it must be passed as
        an SQLebra object to be identified as an existing object. Otherwise, a copy of the multi object will be
        allocated in the database.
        """
        if _id is None:  # Draft new object
            objs_dict = {'type': cls.pyclass.__name__}
            _id = db_draft._free_id()[0]
            objs_dict['id'] = _id
            db_draft._objs.append(objs_dict)
        items_dict = {'ind': tuple(_ind_offset + n for n in range(len(value)))}
        items_dict['id'] = (_id,) * len(items_dict.get('ind', items_dict.get('key')))
        # Go through items, drafting and retrieving their id
        items_dict['child_id'] = []
        for v in value:  # v = value[0]  # value = [0, 0, [0, 1]]
            v_id = db_draft._draft_obj(v)
            items_dict['child_id'].append(v_id)
        db_draft._items.append(items_dict)
        return _id

    def _item2where(self, item):
        """Validate item and return a dictionary to select it from table Items

        Parameters
        ----------
        item: int
            Selected item(s).

        Returns
        -------
        dict
            Dictionary used to select the items' rows from table Items
        """
        len_self = len(self)
        if isinstance(item, int):
            if not -len_self <= item < len_self:
                raise IndexError('SQLebra.{} index out of range'.format(self.__class__.__name__))
            if item < 0:
                return len_self - item, {'id': self._id, 'ind': len_self - item}, False
            else:
                return item, {'id': self._id, 'ind': item}, False
        elif isinstance(item, slice):
            start, stop, step = item.indices(len_self)
            return slice(start, stop, step), \
                   {
                       'id': self._id,
                       '!@*': '(ind >= {start}) and (ind < {stop}) and ((ind - {start}) % {step} == 0)'.format(
                           start=start, stop=stop, step=step)
                   }, True
        else:
            raise TypeError('SQLebra.{} indices must be integers or slices, not {}'.format(self.__class__.__name__,
                                                                                           item.__class__.__name__))

    # [multi; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    _Multi__col_item = 'ind'  # Name of the column containing the indexing item ("key" or "ind")
    _Multi__item_class = SequenceItem

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # Sequence
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [Sequence] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [Sequence; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pass

    # [Sequence; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [Sequence; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # [Sequence] FD
    # ==================================================================================================================
    # Methods/properties fully defined here. They can be overloaded by inheriting classes

    # [Sequence; FD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    def index(self, x, *args):
        """
        Return zero-based index in the list of the first item whose value is equal to x. Raises a ValueError if
        there is no such item.

        The optional arguments start and end are interpreted as in the slice notation and are used to limit the
        search to a particular subsequence of the list. The returned index is computed relative to the beginning of the
        full sequence rather than the start argument.

        Parameters
        ----------
        x: any
            Value to search
        start: int
            Start index boundary in sequence
        end: int
            End index boundary in sequence

        Returns
        -------
        int:
            Index of first instance of `x` in the sequence
        """
        if isinstance(x, object_):
            # If the object is in the same database, find items pointing to it (i.e. child_id == x.id)
            if x.db == self.db:  # Find items pointing to x.id
                if len(args) == 0:
                    where = {}
                elif len(args) == 1:
                    where = {'!@*': 'ind >= {}'.format(args[0])}
                elif len(args) == 2:
                    where = {'!@*': 'ind between {} and {}'.format(args[0], args[1])}
                try:
                    return self.db._items[{'id': self._id, 'child_id': x._id, **where}, 'ind'][0][0]
                except ex.ItemError:  # Try with the value
                    x = x.py
            else:  # Different database. Extract value and count
                x = x.py
        # Compare with external value
        try:
            return self.py.index(x, *args)
        except ValueError:
            raise ValueError('{} {} is not in sqlebra.{}'.format(self.info, x, self.pyclass.__name__))

    def __add__(self, other):
        if isinstance(other, object_):
            return self.py + other.py
        else:
            return self.py + other

    def __iadd__(self, other):
        return self._Sequence__extend(other)

    def count(self, x):
        """
        Return the number of times x appears in the sequence.

        Parameters
        ----------
        x: any
            Value to count.

        Returns
        -------
        int:
            Number of times `x` appears on the sequence
        """
        if isinstance(x, object_):
            # If the object is in the same database, find items pointing to it (i.e. child_id == x.id)
            if x.db == self.db and x not in (True, False, 0, 1):  # Find items pointing to x.id
                return self.db._items[{'id': self._id, 'child_id': x._id}, 'count(*)'][0]
            # else:  # Different database. Extract value and count
            x = x.py
        # Compare with external value
        return self.py.count(x)

    # [Sequence; FD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [Sequence; FD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    def _Sequence__extend(self, x):
        """
        Extend current sequence with the provided sequence.

        Parameters
        ----------
        x: `sequence`
            Sequence to append at the end of the current sequence

        Returns
        -------
        Sequence:
            self

        Notes
        -----
        extend is **not** defined for tuple, but `_extend_`'s functionality is used by tuple.
        """
        # Retrieve ids of appended objects
        if isinstance(x, Sequence):
            if x.db == self.db:  # Same database, use existing object ids
                ids = x.objs
                # Insert list items
                len_x = len(ids)
            else:  # Different database, add new objects
                x = x.py
        # Add to the end
        self._draft_obj(self.db._draft, x, _id=self._id, _ind_offset=len(self))
        self.db._draft._commit()
        return self


def method_generator(fcn):
    """Generates methods to be added to tuple_list_"""

    def method(self, *args, **kwargs):
        return fcn(self.py, *args, **kwargs)

    method.__name__ = fcn.__name__
    return method


# Add methods that are not supported natively
for method in (any, min, max, map, sorted, sum):
    cls_method = method_generator(method)
    setattr(Sequence, cls_method.__name__, cls_method)
