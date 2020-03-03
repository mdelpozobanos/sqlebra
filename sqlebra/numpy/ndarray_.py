from functools import reduce
from operator import mul
import numpy as np
from sqlebra import exceptions as ex
from ..object.object_ import object_
from ..object.nested import Nested, NestedIterator, NestedView
from ..object.item import Item
from .indexing import unravel


class ndarrayView(NestedView):

    @property
    def dtype(self):
        # TODO: Add support to complex dtypes
        return self.obj.dtype

    @property
    def size(self):
        return reduce(mul, self.shape)

    @property
    def ndim(self):
        return len(self.shape)

    @property
    def py(self):
        p = self._p
        return np.reshape(np.array([p_n.py for p_n in p], dtype=self.dtype), self.shape)

    @property
    def _p(self):
        """Tuple version of self.p"""
        if self.ind is None:
            return self.obj._p
        if len(self.ind) == 0:
            return tuple()
        else:
            return self.db[{'id': self.id, 'ind': self.ind, 'order_list': self.ind}]

    @property
    def p(self):
        return np.reshape(np.array(self._p), self.shape)

    def __init__(self, obj, item, ind, shape):
        super(ndarrayView, self).__init__(obj, item)
        # Shape is needed as an additional parameter to un-flat the array
        self.ind = ind
        self.shape = shape

    def __getitem__(self, item):
        # 1. Get parameters
        dtype, shape, ind = unravel(self.dtype, self.shape, item)
        if self.ind is not None:
            ind = [self.ind[i] for i in ind]
        # 2. Apply indexing
        if len(shape) == 0:
            return self.db[{'id': self.id, 'ind': ind}][0]
        else:  # Return a view of the array
            return ndarrayView(self, item, ind, shape)

    def __len__(self):
        return self.shape[0]

    # ndarray methods ==================================================

    def flatten(self):
        return ndarrayView(self, ..., self.ind, self.size)

    def tolist(self):
        return self.p.tolist()


class ndarrayIterator(NestedIterator):

    def __init__(self, obj):
        self.obj = obj
        self.current = -1
        # Set high to size instead of length
        self.high = self.obj.shape[0]

    def __next__(self):
        self.current += 1
        if self.current < self.high:
            obj = self.obj[self.current]
            obj.ref = Item(self.obj, ind=self.current)
            return obj
        self.current = None
        raise StopIteration


class ndarray_(Nested):
    """SQL object of type list"""

    pyclass = np.ndarray
    col_item = 'ind'
    _iterator_class = ndarrayIterator

    @property
    def py(self):
        # Retrieve data
        d = [p_i.py for p_i in self._p]
        return np.reshape(np.array(d, dtype=self.dtype), self.shape)

    @py.setter
    def py(self, x):
        if len(x.dtype) > 0:
            raise NotImplemented('numpy structured arrays are not currently supported by SQLebra')
        if not isinstance(x, self.pyclass):
            x = np.array(x)
        # Empty current
        self.clear()
        # Save properties:
        # + dtype
        id = list(self.db.free_id())
        self.db[id] = (x.dtype.descr, )
        self.db.insert(self.db.tab_items, {'id': self.id, 'key': 'dtype', 'child_id': id[0]})
        # + shape
        id = list(self.db.free_id())
        self.db[id] = (x.shape, )
        self.db.insert(self.db.tab_items, {'id': self.id, 'key': 'shape', 'child_id': id[0]})
        # Save data
        len_x = x.size
        ids = list(self.db.free_id(len_x))
        self.db[ids] = x.flatten().tolist()
        # Insert list items
        insert_items = {'id': (self.id,) * len_x,
                        self.col_item: tuple(range(len_x)),
                        'child_id': tuple(ids)}
        self.db.insert(self.db.tab_items, insert_items)

    @property
    def _p(self):
        """Tuple version of self.p"""
        return self.db[{'id': self.id, '*': 'ind is not NULL'}]

    @property
    def p(self):
        """Half way through py property. List of SQLebra objects"""
        return np.reshape(np.array(self.db[{'id': self.id, '*': 'ind is not NULL'}]), self.shape)

    @property
    def _shape(self):
        shape = self.db[{'id': self.id, 'key': 'shape'}]
        try:
            # Link to object
            shape[0].ref = self
            return shape[0]
        except IndexError:
            if len(shape) == 0:  # Empty array
                return None
            elif len(shape) > 1:
                raise ex.CorruptedDatabase("{} Multiple 'shape' key item".format(self.sql_info))
            else:
                raise

    @property
    def shape(self):
        shape = self._shape
        if shape:
            return self._shape.py
        else:
            return (0, )

    @property
    def _dtype(self):
        """Retruns dtype SQLebra object"""
        dtype = self.db[{'id': self.id, 'key': 'dtype'}]
        try:
            dtype[0].ref = self
            return dtype[0]
        except IndexError:
            if len(dtype) == 0:  # Empty array
                return None
            elif len(dtype) > 1:
                raise ex.CorruptedDatabase("{} Multiple 'dtype' key item".format(self.sql_info))
            else:
                raise

    @property
    def dtype(self):
        """Retruns dtype ready to use"""
        _dtype = self._dtype
        if _dtype:
            dtype = _dtype.py
        else:  # Empty array
            return np.dtype('float64')
        # Normalize dtype
        if len(dtype) == 1:  # Simple dtype
            return np.dtype(dtype[0][1])
        # else:  # Complex dtype
        return [(dtype_n[0], np.dtype(dtype_n[1])) for dtype_n in dtype]

    @property
    def size(self):
        return self.db.select(self.db.tab_items, column=('count(*)', ),
                              where={'id': self.id, '*': 'ind is not NULL'})[0][0]

    def __init__(self, *args, **kwargs):
        super(Nested, self).__init__(*args, **kwargs)

    def __getitem__(self, item):
        # 1. Calculate selected indices and shape
        dtype, shape, ind = unravel(self.dtype, self.shape, item)
        # 2. Apply indexing
        if len(shape) == 0:
            return self.db[{'id': self.id, 'ind': ind}][0]
        else:  # Return a view of the array
            return ndarrayView(self, item, ind, shape)

    def __setitem__(self, item, value):
        # Retrieve selected item
        dtype, shape, ind = unravel(self.dtype, self.shape, item)
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
                if not(value.shape == self_shape[:value.ndim]):
                    raise ValueError(
                        'could not broadcast input array from shape {} into shape {}'.format(shape, value.shape))
                if value.ndim < len(self_shape):  # Broadcast value
                    value = np.broadcast_to(value, self_shape)
            else:  # Broadcast value
                value = np.broadcast_to(value, shape)
            # Finally, flatten value and convert to list
            value = value.flatten().tolist()
        else:  # Broadcast value
            value = [value]*len_ind
        # Get item
        rows = self.db[{'id': self.id, self.col_item: ind}]
        # Edit rows
        try:
            for ind, row, val in zip(ind, rows, value):
                row.ref = Item(self, ind=ind)
                try:  # Update row
                    row.py = val
                except ex.TypeError:  # Try to convert to data type
                    if isinstance(val, object_):
                        row.py = getattr(np, self.dtype.name)(val.py).item()
                    else:
                        row.py = getattr(np, self.dtype.name)(val).item()
        except IndexError:
            if len(rows) == 0:
                raise ex.CorruptedDatabase('Item {} not found in the SQL database'.format(item))
            else:
                raise

    def _check_item_(self, item):
        return unravel(self.dtype, self.shape, item)[1:]

    def __len__(self):
        shape = self.db[{'id': self.id, 'key': 'shape'}]
        try:
            return shape[0][0].py
        except IndexError:
            if len(shape) == 0:
                return 0
            else:
                raise ex.CorruptedDatabase("{} Multiple 'shape' key item".fromat(self.sql_info))

    def __iter__(self):
        return self._iterator_class(self)

    def clear(self, del_ref=True):
        # Clear child_ids.
        # Optimization: Force del_ref to False and, if required, delete all references in one query
        super(ndarray_, self).clear(del_ref=del_ref)
        # Delete items
        if del_ref:
            self.db.delete(self.db.tab_items, where={'id': self.id})

    # extended ndarray methods
    # =============================================================================

    def append(self, values, axis=0):
        if not isinstance(values, (np.ndarray, ndarray_, ndarrayView)):
            values = np.array(values)
        # Assert shape of values matches
        values.shape

    # ndarray methods
    # =============================================================================

    def flatten(self):
        size = self.size
        return ndarrayView(self, ..., None, (size, ))

    def squeeze(self, axis=None):
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
        return ndarrayView(self, ..., None, shape)


def remove_leading_1s(x):
    ind = next((i for i, x_i in enumerate(x) if x_i > 1), None)
    return x[ind:]
