from functools import reduce
from operator import mul
import numpy as np
from sqlebra import exceptions as ex
from sqlebra.object.object_ import object_
from sqlebra.object.multi import MultiItem, Multi
from .indexing import unravel
from ..python.sequence import _Sequence__SequenceSlice_generator

SequenceSlice = _Sequence__SequenceSlice_generator(list)


class ndarrayTolist(SequenceSlice):

    def __init__(self, ref):
        """
        Parameters
        ----------
        ref: `sqlebra.object`
            SQLebra object owning the item.
        """
        super().__init__(ref, None, None)
        super(SequenceSlice, self).__init__(
            ref.objs.tolist()
        )


class ndarrayItem(MultiItem):
    pass


class ndarray_(Multi):
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

    pyclass = np.ndarray  # Python class

    # Python values
    # -------------

    @property
    def py(self):
        """Python value of the object"""
        objs = self._Multi__objs_tuple
        if len(objs) > 0:
            return np.reshape(np.array(tuple(o.py for o in objs), dtype=self.dtype), self.shape)
        else:
            return np.array([])

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

    pass

    # [multi; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    @classmethod
    def _draft_obj(cls, db_draft, value, _id=None):
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

        Returns
        -------
        int:
            Unique object identifier pertaining to the drafted object. If the object is new, the identifier will be < 0.

        Notes
        -----
        Multi objects are defined by multiple rows across tables OBJECTS and ITEMS. The definition of this procedure is
        fully decentralised since multi classes may follow very difference drafting procedures.

        Sequence objects are not directly hashable in SQLebra. If a sequence object needs to be reused, it must be
        passed as an SQLebra object to be identified as an existing object. Otherwise, a copy of the sequence object
        will be allocated in the database.
        """
        if _id is None:  # Draft new object
            objs_dict = {'type': cls.pyclass.__name__}
            _id = db_draft._free_id()[0]
            objs_dict['id'] = _id
            db_draft._objs.append(objs_dict)
        # Add shape
        shape_id = db_draft._draft_obj(value.shape)
        db_draft._items.append({'id': _id, 'key': 'shape', 'child_id': shape_id})
        # Add dtype
        dtype_id = db_draft._draft_obj(value.dtype.descr)
        db_draft._items.append({'id': _id, 'key': 'dtype', 'child_id': dtype_id})
        # Add values
        items_dict = {'id': (_id,) * value.size,
                      'ind': tuple(range(value.size)),
                      'child_id': []}
        for v in value.flatten():  # v = value.flatten()[0]
            if isinstance(v, (np.ndarray, np.generic)):
                v_id = db_draft._draft_obj(v.item())
            else:
                v_id = db_draft._draft_obj(v)
            items_dict['child_id'].append(v_id)
        db_draft._items.append(items_dict)
        return _id

    def _item2where(self, item):
        """Validate item and return a dictionary to select it from table Items

        Parameters
        ----------
        item: str
            Selected item(s) (key).

        Returns
        -------
        dict
            Dictionary used to select the items' rows from table Items
        """
        raise NotImplemented
        if item in self:
            return item, {'id': self._id, 'key': item}
        else:
            raise KeyError('{}'.format(item))
        return unravel(self.dtype, self.shape, item)[1:]

    # [multi] Overloaded
    # ==================================================================================================================
    # Methods/properties overloaded

    # [multi; overloaded] SQLebra public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    def __getitem__(self, item):
        # 1. Calculate selected indices and shape
        dtype, shape, ind = unravel(self.dtype, self.shape, item)
        # 2. Apply indexing
        if len(shape) == 0:
            return self.db._items.obj[self, ind[0]]
        else:  # Return a view of the array
            return ndarrayView(self, item, ind, shape)

    # [multi; overloaded] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    _Multi__col_item = 'ind'  # Name of the column containing the indexing item ("key" or "ind")
    _Multi__item_class = ndarrayItem

    @property
    def _Multi__objs_tuple(self):
        """Tuple with all objects"""
        return self.db._items.obj[self, {'id': self._id, '!@*': 'key is NULL'}]

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

    @property
    def objs(self):
        """Half-way-through py property. Sequence of SQLebra objects"""
        key_item_id = self.db._items[{'id': self._id}, ('key', 'child_id',)]
        if len(key_item_id) > 0:
            # Extract keys and item_id
            key = tuple(map(lambda n: n[0], key_item_id))
            # Extract objects
            try:
                objs = self.db._objs.obj[self, tuple(map(lambda n: n[1], key_item_id))]
            except ex.ObjectError:
                raise ex.CorruptedDatabase('Object id {} is corrupted'.format(self._id))
            return np.array(objs[2:]).reshape(objs[1].py)
        else:
            return np.array(tuple())

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # ndarray_
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [ndarray_] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [ndarray_; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pass

    # [ndarray_; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [ndarray_; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # [ndarray_] FD
    # ==================================================================================================================
    # Methods/properties fully defined here. They can be overloaded by inheriting classes

    # [ndarray_; FD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    @property
    def shape(self):
        """ndarray shape"""
        try:
            return self.db._objs.obj[self, self.db._items[{'id': self._id, 'key': 'shape'}, 'child_id'][0][0]].py
        except ex.ItemError:  # Empty array
            return (0,)

    @property
    def dtype(self):
        """ndarray dtype"""
        try:
            dtype = self.db._objs.obj[self, self.db._items[{'id': self._id, 'key': 'dtype'}, 'child_id'][0][0]].py
        except ex.ItemError:  # Empty array
            return np.dtype('float64')
        else:
            if len(dtype) == 1:  # Simple dtype
                return np.dtype(dtype[0][1])
            else:  # Complex dtype
                return [(dtype_n[0], np.dtype(dtype_n[1])) for dtype_n in dtype]

    @property
    def size(self):
        return self.db._items[{'id': self._id, '!@*': 'ind is not NULL'}, 'count(*)'][0][0]

    @property
    def ndim(self):
        return len(self.shape)

    def __setitem__(self, item, value):
        row = self[item]
        if isinstance(row, ndarrayView):
            row[:] = value
        else:
            row._ref.py = value

    def __len__(self):
        return self.db._objs.obj[self, self.db._items[{'id': self._id, 'key': 'shape'}, 'child_id'][0][0]][0].py

    def any(self):
        return self.py.any()

    def all(self):
        return self.py.all()

    def flatten(self):
        return ndarrayView(self, ..., None, (np.prod(self.shape), ))

    def tolist(self):
        return ndarrayTolist(self)

    def squeeze(self, axis=None):
        """Remove axes of length one from array.
        Parameters
        ----------
        axis: None or int or tuple of ints, optional
            Selects a subset of the entries of length one in the shape. If an axis is selected with shape entry greater
            than one, an error is raised.

        """
        shape = self.shape
        if axis is None:  # Squeeze all axes

            shape = [s for s in shape if s > 1]

        elif isinstance(axis, int):  # One index selected

            try:
                if shape[axis] == 1:
                    raise ValueError('cannot select an axis to squeeze out which has size not equal to one')
            except IndexError:
                if axis > len(shape):
                    raise np.AxisError(
                        'axis {} is out of bounds for array of dimension {}'.format(axis, len(shape)))
                # Unidentified error
                raise
            shape = shape[:axis] + shape[axis + 1:]

        elif isinstance(axis, tuple):  # Multiple axes selected

            try:
                if not all(shape[a] for a in axis):
                    raise ValueError('cannot select an axis to squeeze out which has size not equal to one')
            except IndexError:
                for a in axis:
                    if a > len(shape):
                        raise np.AxisError('axis {} is out of bounds for array of dimension {}'.format(a, len(shape)))
                # Unidentified error
                raise
            # Compute shape
            shape = tuple(shape[a] for a in range(len(shape)) if a not in axis)

        else:
            raise TypeError('axis must be int or tuple of ints')
        return ndarrayView(self, ..., None, tuple(shape))

    def __array__(self, *args, **kwargs):
        """See numpy.ndarray.__array__"""
        return self.objs

    # Extra methods
    # -------------

    def append(self, values, axis=None):
        """Append values to the end of an array.

        Parameters
        ----------
        values
        axis

        Returns
        -------

        """
        # TODO: Implement this
        raise NotImplementedError
        if not isinstance(values, (np.ndarray, ndarray_, ndarrayView)):
            values = np.array(values)
        # Assert shape of values matches
        values.shape

    # [ndarray_; FD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [ndarray_; FD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass


# Support functions
def py_setter(o, value):
    """Set SQLebra object value"""
    o.py = value


class ndarrayView(ndarray_):
    """View class for SQLebra's ndarray_"""

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # object_
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [object_] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [object_; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pyclass = ndarray_.pyclass

    # Python values
    # -------------

    @property
    def py(self):
        """Python value of the object"""
        return np.array(tuple(map(lambda x: x.py, self._Multi__objs_tuple)), dtype=self.dtype).reshape(self.shape)

    @py.setter
    def py(self, value):
        self[:] = value

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

    pass

    # [multi; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    @property
    def _draft_obj(self):
        return self._ref._draft_obj

    # [Sequence] Overloaded
    # ==================================================================================================================
    # Methods/properties overloaded

    # [multi; overloaded] SQLebra public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    def __getitem__(self, item):
        # 1. Get parameters
        dtype, shape, ind = unravel(self.dtype, self.shape, item)
        if self.ind is not None:
            ind = [self.ind[i] for i in ind]
        # 2. Apply indexing
        if len(shape) == 0:
            return self.db._items.obj[self._Multi__item_class(ref=self, ind=ind), {'id': self._id, 'ind': ind}][0]
        else:  # Return a view of the array
            return ndarrayView(self, item, ind, shape)

    # [multi; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    _Multi__col_item = ndarray_._Multi__col_item
    _Multi__item_class = ndarray_._Multi__item_class

    @property
    def _Multi__objs_tuple(self):
        """Tuple version of self.p"""
        if self.ind is None:  # Full view
            return self._ref._Multi__objs_tuple
        elif len(self.ind) == 0:  # Empty view
            return tuple()
        elif self._ndarrayView__ind:
            aux = self.db._items.obj[self, self.ind]
            return tuple(aux[n] for n in self._ndarrayView__ind)
        else:  # Selected items
            return self.db._items.obj[self, self.ind]

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

    @property
    def objs(self):
        """Half-way-through py property. Object of type `self.pyclass` where values are SQLebra objects"""
        return np.reshape(np.array(self._Multi__objs_tuple), self.shape)

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # ndarray_
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [ndarray_] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [ndarray_; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pass

    # [ndarray_; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [ndarray_; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # [ndarray_] FD
    # ==================================================================================================================
    # Methods/properties fully defined here. They can be overloaded by inheriting classes

    # [ndarray_; FD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    @property
    def shape(self):
        """ndarray shape"""
        try:
            return self.db._objs.obj[self, self.db._items[{'id': self._id, 'key': 'shape'}, 'child_id'][0]].py
        except ex.ItemError:  # Empty array
            return (0,)

    @property
    def dtype(self):
        """ndarray dtype"""
        # TODO: Add support to complex dtypes
        return self._ref.dtype

    @property
    def size(self):
        """ndarray size"""
        return reduce(mul, self.shape)

    @property
    def ndim(self):
        """ndarray ndim"""
        return len(self.shape)

    def __setitem__(self, item, value):
        # 1. Get parameters
        dtype, shape, ind = unravel(self.dtype, self.shape, item)
        if self.ind is not None:
            ind = [self.ind[i] for i in ind]
        len_ind = len(ind)  # Number of assigned items
        # Broadcast value if necessary
        if hasattr(value, '__len__'):  # Nested value
            # Normalize value to numpy array
            if not isinstance(value, (np.ndarray, ndarray_, ndarrayView)):
                value = np.array(value)
            # Remove all leading empty dimensions from value
            first_axis = next((i for i, x in enumerate(value.shape) if x > 1), None)
            if first_axis > 0:
                value = value.squeeze(tuple(range(first_axis)))
            # Check shape
            if value.size > 1:  # Make sure that value.shape matches with self[item].shape (i.e. shape)
                # Remove all leading empty dimensions from self[item].shape
                first_axis = next((i for i, x in enumerate(shape) if x > 1), None)
                self_shape = shape[first_axis:]
                if not (value.shape == self_shape[:value.ndim]):
                    raise ValueError(
                        'could not broadcast input array from shape {} into shape {}'.format(shape, value.shape))
                if value.ndim < len(self_shape):  # Broadcast value
                    value = np.broadcast_to(value, self_shape)
            else:  # Broadcast value
                value = np.broadcast_to(value, shape)
            # Finally, flatten value and convert to list
            value = value.flatten().tolist()
        else:  # Broadcast value
            value = (value,) * len_ind
        # Edit values
        objs = self.db._items.obj[self, ind]
        tuple(map(py_setter, objs, value))

    def __len__(self):
        """ndarray len"""
        return self.shape[0]

    def any(self):
        return self.py.any()

    def all(self):
        return self.py.all()

    @property
    def shape(self):
        return self._shape

    # [ndarray_; FD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [ndarray_; FD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # ndarrayView
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [ndarrayView] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [ndarrayView; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pass

    # [ndarrayView; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [ndarrayView; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # [ndarrayView] FD
    # ==================================================================================================================
    # Methods/properties fully defined here. They can be overloaded by inheriting classes

    # [ndarrayView; FD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    def __init__(self, ref, item, ind, shape):
        """
        Parameters
        ----------
        ref: ndarray_ or ndarrayView
            Calling ndarray object
        item: int, list, tuple or slice
            Indexing object
        ind: tuple
            Indices selected by item
        shape: tuple
            Shape of resulting view
        """
        super().__init__(db=ref.db, id=ref._id, ref=ref)
        self.item = item
        self.ind = ind
        self._shape = shape  # Cannot assign directly to `self.shape`
        if ind is not None and len(ind) > len(set(ind)):
            sorted_ind = tuple(set(sorted(self.ind)))
            self._ndarrayView__ind = tuple(sorted_ind.index(x) for x in self.ind)
        else:
            self._ndarrayView__ind = False

    # [ndarrayView; FD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [ndarrayView; FD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    # ndarray methods ==================================================

    def flatten(self):
        return ndarrayView(self, ..., self.ind, (np.prod(self.shape), ))

    def tolist(self):
        return ndarrayTolist(self)
