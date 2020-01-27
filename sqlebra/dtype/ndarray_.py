from ..object.nested import Nested
import numpy as np
import copy
from sqlebra import exceptions as ex


class ndarray_(Nested):
    """SQL object of type list"""

    pyclass = np.ndarray
    col_item = 'ind'

    @property
    def py(self):
        x = []
        if self._slice is None:
            try:
                rows = self.db[{'id': self.id, '*': 'root = 0'}][2]
            except ex.VariableError:  # Empty list
                # Possibly an empty list. Retrieve root to assert
                self.db[{'id': self.id, '*': 'root = 1'}]
            else:
                return np.reshape(np.array([r.py for r in rows[2:]], dtype=self.dtype), self.shape)
        else:
            try:
                rows = super(ndarray_, self).__getitem__(self._slice['ind'])[2]
            except:  # Empty list
                return np.array([])
            else:
                return np.reshape(np.array([r.py for r in rows], dtype=self.dtype), self.shape)

    @py.setter
    def py(self, x):
        if len(x.dtype) > 0:
            raise NotImplemented('numpy structured arrays are not currently supported by SQLebra')
        if not isinstance(x, self.pyclass):
            x = np.array(x)
        # Empty current
        self.clear()
        # Insert new values
        value_item = {'id': self.id, 'root': False, 'user_defined': False}
        # Save properties:
        # + dtype
        property_item = value_item.copy()
        property_item['key'] = 'dtype'
        property_item['name'] = self._nameitem_('dtype')
        value = x.dtype.descr
        self.db[property_item] = value
        # + shape
        shape = x.shape
        property_item['key'] = 'shape'
        property_item['name'] = self._nameitem_('shape')
        value = shape
        self.db[property_item] = value
        # Save data
        for ind, value in enumerate(x.flatten()):
            if not isinstance(value, np.ndarray):
                value = value.item()
            value_item['name'] = self._nameitem_(ind, shape)
            value_item[self.col_item] = ind
            self.db[value_item.copy()] = value

    @property
    def shape(self):
        if self._slice is None:
            return self.db[{'id': self.id, 'key': 'shape'}][2][0].py
        else:
            return self._slice['shape']

    @property
    def dtype(self):
        dtype = self.db[{'id': self.id, 'key': 'dtype'}][2][0].py
        dtype = [(dtype_n[0], np.dtype(dtype_n[1])) for dtype_n in dtype]
        if len(dtype) == 1:
            return dtype[0][1]
        else:
            return dtype

    def __init__(self, *args, **kwargs):
        super(Nested, self).__init__(*args, **kwargs)
        # _slice is used as a mask. It can be set to:
        # + {
        #       'ind': List of masked ravelled indices
        #       'shape': Shape of the selected slice
        #   }
        self._slice = None

    def __getitem__(self, item):
        item, res_shape = self._check_item_(item)
        if self._slice is not None:
            item = [self._slice['ind'][i] for i in item]
        if len(res_shape) == 0:
            return super(ndarray_, self).__getitem__(item)[2][0]
        else:  # Return a slice of the array
            x = copy.copy(self)
            x._slice = {'ind': item, 'shape': res_shape}
            return x

    def __setitem__(self, key, value):
        # Retrieve selected item
        item, item_shape = self._check_item_(key)
        # Expand value if necessary
        if len(item) > 0:
            if hasattr(value, '__len__'):
                if len(value) == 1:
                    value *= len(item)
                elif len(value) != len(item):
                    if len(item) == 1:
                        raise ValueError('setting an array element with a sequence')
                    else:
                        raise ValueError('cannot copy sequence with size {} to array axis with dimension {}'.format(
                            np.array(value).shape, item_shape
                        ))
            else:
                value = [value]*len(item)
        # Prepare row
        rows = self.db[{'id': self.id, self.col_item: item}][2]
        if len(rows) == 0:
            raise ex.CorruptedDatabase('Item {} not found in the SQL database'.format(key))
        # Retrieve row
        for row, val in zip(rows, value):
            try:  # Update row
                row.py = val
            except TypeError:  # Try to convert to data type
                row.py = getattr(np, self.dtype.name)(val).item()

    def _check_item_(self, item):
        """
        Check and ravel indices
        :param item: Indices used to index a np.ndarray
        :return: a list of integers with ravelled indices and a tuple with the shape of the result of indexing
        """
        # str items can be used in structured arrays
        if isinstance(item, str):
            raise NotImplemented('numpy structured arrays not supported')
            dtype = self.dtype
            if len(dtype) == 0:
                raise ValueError
            else:
                return [col_n for col_n, dtype_n in enumerate(dtype) if dtype_n[0] == item], self.shape
        shape = self.shape
        ndim = len(shape)  # Number of dimensions of calling array
        # Normalize item to list
        if isinstance(item, list):
            item = [item, ]
        elif isinstance(item, tuple):
            item = list(item)
        else:
            item = [item]
        # Expand item to full index coordinates
        # item += [slice(None)] * (ndim - len(item))
        n_item = 0
        d_array = 0
        res_shape = []
        is_list = [False]*ndim
        len_item = len(item)
        while n_item < len_item or d_array < ndim:
            if n_item == len_item:  # No more indices
                item.append(np.arange(shape[d_array]))
                res_shape.append(shape[d_array])
                d_array += 1
                continue
            if item[n_item] is None:  # Inserted dimension
                res_shape.append(1)
                n_item += 1
                continue
            elif isinstance(item[n_item], int):  # Removed dimension
                # Check valid index
                item[n_item] = np.array([_check_index_(d_array, shape[d_array], item[n_item])])
            elif isinstance(item[n_item], slice):
                # Expand slice
                item[n_item] = np.arange(shape[d_array])[item[n_item]]
                res_shape.append(len(item[n_item]))
            elif isinstance(item[n_item], list) or isinstance(item[n_item], tuple):
                is_list[n_item] = True
                # Valid indices
                item[n_item] = np.array([_check_index_(d_array, shape[d_array], i) for i in item[n_item]])
                res_shape.append(len(item[n_item]))
            n_item += 1
            d_array += 1
        # All lists must have the same length
        if any(is_list):
            len_lists = np.array([len(i) for l, i in zip(is_list, item) if l])
            if len(len_lists) > 1 and not all(len_lists == len_lists[0]):
                raise IndexError
        # Total number of coordinate tuples
        num_ind = np.prod(res_shape)
        if num_ind == 0:  # Empty slice
            return [], (0, )
        # Remove None items
        item = [i for i in item if i is not None]
        item_shape = [len(i) for i in item]
        for d in range(len(item)):  # d = 0
            len_d = item_shape[d]
            if len_d < num_ind:
                if num_ind % len_d != 0:
                    raise IndexError
                # item[d] = np.tile(item[d], int(num_ind / len_d))
                item_d_shape = np.ones(ndim, dtype=int)
                item_d_shape[d] = len_d
                item[d] = np.broadcast_to(np.reshape(item[d], item_d_shape), item_shape).flatten()
        return np.ravel_multi_index(item, shape).tolist(), tuple(res_shape)

    def __len__(self):
        if self._slice is None:
            child_id = self.db.select(column=['child_id'], where={'id': self.id, 'key': 'shape'})[0][0]
            return self.db.select(column=['int_val'], where={'id': child_id, 'ind': 0})[0][0]
        else:
            return self._slice['shape'][0]

    def _nameitem_(self, item, shape=None):
        """Overloaded method that allows unravelling indices"""
        if shape is None:
            return super(ndarray_, self)._nameitem_(item)
        if isinstance(item, int):
            return "{}{}".format(self.name, list(np.unravel_index(item, shape)))
        else:
            raise TypeError


def _check_index_(dim, dim_size, ind):
    """
    Check integer index is valid and convert to positive integer
    :param dim: (int) Corresponding dimension (used to rise error)
    :param dim_size: (int) Size of the corresponding dimension
    :param ind: (int) Index to check
    :return: (int) Positive index
    """
    if ind < 0:
        ind = dim_size + ind
    if ind < 0 or ind >= dim_size:
        raise ValueError("Index {} out of bounds for axis {} with size {}".format(
            ind, dim, dim_size))
    return ind