import random
import string
from . import exceptions as mis
# from sqlebra.sqlite import sqlitedb


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
