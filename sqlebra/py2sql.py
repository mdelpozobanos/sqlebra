from sqlebra.python import NoneType_
from sqlebra.python import int_
from sqlebra.python import float_
from sqlebra.python import bool_
from sqlebra.python import str_
from sqlebra.python import list_
from sqlebra.python import tuple_
from sqlebra.python import dict_
from sqlebra.numpy import ndarray_

# from sqlebra.dtype.function import SQLfunction

# List of SQLebra objects
single = (int_, float_, bool_, str_, NoneType_)
nested = (list_, tuple_, dict_, ndarray_)

# Dictionaries defining the relationship between python and sqlebra classes

py2sql_single = {}
py2sql_single_str = {}
for c in single:
    py2sql_single[c.pyclass] = c
    py2sql_single_str[c.pyclass.__name__] = c

py2sql_nested = {}
py2sql_nested_str = {}
for c in nested:
    py2sql_nested[c.pyclass] = c
    py2sql_nested_str[c.pyclass.__name__] = c
