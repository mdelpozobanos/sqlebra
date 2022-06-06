# TODO: Review script once SQLebra completed

import random
import string
import numpy as np
# from . import py2sql, _variables


def random_str(str_len=10):
    """Generate a random string of fixed length"""
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(str_len))


# def value2row(x):
#     """Return the result of calling `value_row` method from the SQLebra class associated with python's variable `x`"""
#     if type(x) in py2sql.py2sqlebra_single:
#         return py2sql.py2sqlebra_single[type(x)]._value2row(x)
#     elif type(x) in py2sql.py2sqlebra_multi:
#         return py2sql.py2sqlebra_multi[type(x)]._value2row(x)


# def row2value(db, row):
#     """Return the result of calling `row2value` method from the SQLebra class associated with python's variable `x`"""
#     class_name = row[_variables.COL_DICT['class']]
#     try:
#         return py2sql.py2sql_single_str[class_name]._row2value(db, row)
#     except KeyError:
#         return py2sql.py2sql_nested_str[class_name]._row2value(db, row)


def argsort(x):
    """Return indices sorting iterable x"""
    return sorted(range(len(x)), key=x.__getitem__)


def rank_list(x, break_ties=False):
    """Indices ranking iterable x"""
    n = len(x)
    t = list(range(n))
    s = sorted(t, key=x.__getitem__)

    if not break_ties:
        for k in range(n-1):
            t[k+1] = t[k] + (x[s[k+1]] != x[s[k]])

    r = s.copy()
    for i, k in enumerate(s):
        r[k] = t[i]

    return r


def rank_vec(x, break_ties=False):
    """Indices ranking numpy array x"""
    n = len(x)
    t = np.arange(n)
    s = sorted(t, key=x.__getitem__)

    if not break_ties:
        t[1:] = np.cumsum(x[s[1:]] != x[s[:-1]])

    r = t.copy()
    np.put(r, s, t)
    return r
