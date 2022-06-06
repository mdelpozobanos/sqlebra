from functools import reduce
from operator import mul
import itertools
import numpy as np

# INDEXING_SIZE is a thresholds controlling the indexing method used.
#   array <= INDEXING_SIZE
#       Allocate a dummy array and indexing to infer the indices and shape of the resulting array
#   array > INDEXING_SIZE
#       Manually compute the indices and shape of the resulting array
INDEXING_SIZE = 1000


def check_index(dim, dim_size, ind):
    """
    Check if an int index is valid and normalize to positive int

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


def unravel(dtype, shape, item, mth=1):
    """
    Converts a tuple of index arrays into an array of flat indices, supporting non-integer indexing (e.g. slices).

    :param dtype: (tuple)
    :param shape: (tuple)
    :param item: (tuple, list, array, int, slice or ellipsis)
    :param mth: (int) Selected method

    :return: A tuple with the dtype of the resulting array
             A tuple with the shape of the resulting array
             A tuple of flat indices
    """

    if mth == 1:
        size = reduce(mul, shape)
        return _unravel_mth1_(dtype, shape, size, item)
    elif mth == 2:
        _unravel_mth2_(dtype, shape, item)
    elif mth == 3:
        _unravel_mth3_(dtype, shape, item)
    elif mth == 0:  # Automatic

        # TODO: str items can be used in structured arrays
        if isinstance(item, str):
            raise NotImplemented('numpy structured arrays not supported')

        # Compute size of array
        size = reduce(mul, shape)

        # Check for advance indexing
        if isinstance(item, tuple):
            advance_ind = tuple(
                i for i, item_i in enumerate(item)
                if isinstance(item_i, list) or isinstance(item_i, tuple) or isinstance(item_i, np.ndarray)
            )
            if len(advance_ind) > 0:  # Too complex, use method 1
                return _unravel_mth1_(dtype, shape, size, item)
        if size <= INDEXING_SIZE:  # Small array, use method 1 (faster)
            return _unravel_mth1_(dtype, shape, size, item)
        # Use other method
        return _unravel_mth2_(dtype, shape, item)


def _unravel_mth1_(dtype, shape, size, item):
    """
    Unravel indexing by:
    1. Allocating a dummy array with the specified shape filled with indices (i.e. 1, 2, 3,...)
    2. Indexing the above array using item
    3. Extract from the above the indices (from content) and shape.

    Advantages:
    + Support advance indexing
    Disadvantages:
    + May be slower than other methods, particularly for large arrays
    + Memory inefficient
    """
    aux = np.reshape(np.arange(size), shape)[item]
    res_shape = aux.shape
    res_ind = tuple(aux.flatten().tolist())
    return dtype, res_shape, res_ind


def _unravel_mth2_(dtype, shape, item):
    """
    Unravel indexing by:
    1. Manually calculating all int item
    2. Using np.ix_ to expands items
    3. Using np.ravel_multi_index to compute final indices

    Advantages:
    + May be faster than other methods, particularly for large arrays
    + Memory efficient
    Disadvantages:
    + Does not support advance indexing
    """
    int_item = []
    s = 0
    for i, item_i in enumerate(item):  # i = 0;  item_i = item[i]
        if isinstance(item_i, int):  # dimension removed
            int_item.append((check_index(s, shape[s], item_i),))
            s += 1
        elif isinstance(item_i, list) or isinstance(item_i, tuple):    # int indexed dimension
            int_item.append(tuple(check_index(s, shape[s], i) for i in item_i))
            s += 1
        elif isinstance(item_i, slice):  # slice indexed dimension
            aux = tuple(np.arange(shape[s])[item_i].tolist())
            int_item.append(aux)
            s += 1
        elif item_i is None:  # Inserted dimension
            pass
        elif isinstance(item_i, ellipsis):
            raise NotImplemented
    # Expand int_item and compute res_ind
    # Using np.ix_
    res_ind = np.ravel_multi_index(np.ix_(*int_item), shape)
    return dtype, res_ind.shape, res_ind.flatten().tolist()


def _unravel_mth3_(dtype, shape, item):
    """
    Unravel indexing by:
    1. Manually calculating all int item and resulting shape
    2. Using itertools to expands items
    3. Using np.ravel_multi_index to compute the final items

    Advantages:
    + May be faster than other methods, particularly for large arrays
    + Memory efficient
    Disadvantages:
    + Does not support advance indexing
    """
    int_item = []
    res_shape = []
    s = 0
    for i, item_i in enumerate(item):  # i = 0;  item_i = item[i]
        if isinstance(item_i, int):  # dimension removed
            int_item.append((check_index(s, shape[s], item_i),))
            s += 1
        elif isinstance(item_i, list) or isinstance(item_i, tuple):    # int indexed dimension
            int_item.append(tuple(check_index(s, shape[s], i) for i in item_i))
            res_shape.append(len(item_i))
            s += 1
        elif isinstance(item_i, slice):  # slice indexed dimension
            aux = tuple(np.arange(shape[s])[item_i].tolist())
            int_item.append(aux)
            res_shape.append(len(aux))
            s += 1
        elif item_i is None:  # Inserted dimension
            res_shape.append(1)
        elif isinstance(item_i, ellipsis):
            raise NotImplemented
    # Expand int_item and compute res_ind
    int_item = tuple(zip(*tuple(itertools.product(*int_item))))
    # Use np.ravel_multi_index to compute flat indices
    res_ind = np.ravel_multi_index(int_item, shape)
    return dtype, tuple(res_shape), res_ind.tolist()
