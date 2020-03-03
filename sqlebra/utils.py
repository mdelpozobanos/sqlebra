import random
import string
import numpy as np
from . import exceptions as mis


def random_str(str_len=10):
    """Generate a random string of fixed length"""
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(str_len))


def value2row(x):
    """Call value_row method from the relevant SQLebra class"""
    if type(x) in mis.py2sqlebra_single:
        return mis.py2sqlebra_single[type(x)].value2row(x)
    elif type(x) in mis.py2sqlebra_multi:
        return mis.py2sqlebra_multi[type(x)].value2row(x)


def row2value(db, row):
    """Call row2value method from the relevant SQLebra class"""
    class_name_ind = sqlitedb.SQLiteDB._values_dict['class_name']
    for k in mis.py2sqlebra_single.keys():
        if k.__name__ == row[class_name_ind]:
            return mis.py2sqlebra_single[k].row2value(db, row)
    for k in mis.py2sqlebra_multi.keys():
        if k.__name__ == row[class_name_ind]:
            return mis.py2sqlebra_multi[k].row2value(db, row)


def argsort(x):
    """Indices sorting iterable x"""
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
