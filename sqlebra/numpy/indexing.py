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
    res_ind = aux.flatten().tolist()
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


def item_getitem(shape0, item1, shape1, item2, mth=1):
    """
    Computes item3 = item1[item2]

    :param shape0: (tuple) Shape of x0, with x0 the initial array
    :param item1: (tuple) Indexing applied to x0
    :param shape1: (tuple) Shape of x1 = x0[item1]
    :param item2: (tumple) Indexing applied to x1
    :param mth: (int) Selected method

    :return: 'shape2': (tuple) Shape of x2 = x1[item2]
             'item12': (tumple) Indexing so that x0[item12] = x0[item1][item2]
    """
    if mth == 1:
        return _item_getitem_mth1_(shape0, item1, item2)
    else:
        return _item_getitem_mth2_(shape0, item1, shape1, item2)


def _item_getitem_mth1_(shape0, item1, item2):
    """
    Computes item3 = item1[item2] by:
    1. Create a dummy array with shape0
    2. Apply [item1][item2] to the above
    """
    size0 = reduce(mul, shape0)
    aux = np.reshape(np.arange(size0), shape0)[item1][item2]
    shape3 = aux.shape
    item3 = np.unravel_index(aux.flatten(), shape0)
    return shape3, tuple(i.tolist() for i in item3)


def _item_getitem_mth2_(shape0, item1, shape1, item2):

    # Normalize item1
    if isinstance(item1, int) or isinstance(item1, slice):
        item1 = [item1]
    else:
        item1 = list(item1)
    # Normalize item2
    if isinstance(item2, int) or isinstance(item2, slice):
        item2 = [item2]
    else:
        item2 = list(item2)
    # Compute item3
    shape2 = []
    item12 = []
    # itemN pointers
    n1 = 0
    n2 = 0
    # shapeN pointers
    s0 = 0
    s1 = 0
    # While flags
    while1 = True  # n1 < len(item1)
    while2 = True  # n2 < len(item2)
    while while1 or while2:
        # Check for new inserted dimensions by item2
        if while2:
            item2_n = item2[n2]
            if item2_n is None:  # Inserted dimension
                # Insert dimension
                item12.append(None)
                shape2.append(1)
                # Update pointers
                n2 += 1
                continue
        try:
            item1_n = item1[n1]
        except IndexError:
            # TODO: Too many dimensions specified
            raise IndexError
        if isinstance(item1_n, int):  # Deleted dimension
            # Deleted dimension
            item12.append(item1_n)
            # Update pointers
            n1 += 1
            s0 += 1
            continue
        elif item1_n is None:  # Inserted dimension
            if while2:
                if isinstance(item2_n, int):  # Remove dimension
                    if item2_n != 0 and item2_n != -1:
                        # Index out for range for dimension
                        raise IndexError("Index {} out of bounds for axis {} with size {}".format(
                            item2_n, n2, shape1[s1]))
                elif isinstance(item2_n, slice):
                    item12.append(None)
                    shape2.append(1)
                elif isinstance(item2_n, list):
                    a = 10
                else:
                    shape2.append(1)
            else:  # Added dimension
                item12.append(None)
                shape2.append(1)
            # Update pointers
            n1 += 1
            n2 += 1
            s1 += 1
        elif isinstance(item1_n, slice):
            if while2:
                # Retrieve start, stop, step
                slice1 = item1_n.indices(shape0[s0])
                # Calculate select item
                if isinstance(item2_n, int):  # Deleted dimension
                    item2_n = check_index(n2, shape1[s1], item2_n)
                    item12.append(slice1[0] + (item2_n * slice1[2]))
                elif isinstance(item2_n, slice):
                    # Retrieve start, stop, step
                    slice2 = item2_n.indices(shape1[n2])
                    # Combine slices
                    start3 = slice1[0] + slice2[0]*slice1[2]
                    stop3 = slice1[0] + slice2[1]*slice1[2]
                    step3 = slice1[2]*slice2[2]
                    item12.append(slice(start3, stop3, step3))
                    shape2.append(max(0, int((stop3 - start3)/step3)))
                elif isinstance(item2_n, list):
                    raise NotImplemented
                elif isinstance(item2_n, tuple):
                    raise NotImplemented
                # Update pointers
                n1 += 1
                n2 += 1
                s0 += 1
                s1 += 1
            else:  # Simply add item1_n
                item12.append(item1_n)
                shape2.append(shape1[s1])
                # Update pointers
                n1 += 1
                s0 += 1
                s1 += 1
        elif isinstance(item1_n, list) or isinstance(item1_n, tuple):
            if while2:
                # Calculate selected items
                if isinstance(item2_n, int):  # Deleted dimension
                    item12.append(item1_n[item2_n])
                if isinstance(item2_n, slice):
                    item12.append(item1_n[item2_n])
                    shape2.append(len(item12[-1]))
                elif isinstance(item2_n, list) or isinstance(item2_n, tuple):
                    item12.append([i[item2_n] for i in item1_n])
                    shape2.append(len(item12[-1]))
                # Update pointers
                n1 += 1
                n2 += 1
                s0 += 1
                s1 += 1
            else:  # Simply add item1_n
                item12.append(item1_n)
                shape2.append(shape0[s0])
                # Update pointers
                n1 += 1
                s0 += 1
        # Update while flags
        while1 = n1 < len(item1)
        while2 = n2 < len(item2)
    if s0 < len(shape0):
        shape2 += shape0[s0:]
    return tuple(item12), np.array(shape2)



