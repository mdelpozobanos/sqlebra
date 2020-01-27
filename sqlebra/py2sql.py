from sqlebra.dtype import NoneType_
from sqlebra.dtype import int_
from sqlebra.dtype import float_
from sqlebra.dtype import bool_
from sqlebra.dtype import str_
from sqlebra.dtype import list_
from sqlebra.dtype import tuple_
from sqlebra.dtype import dict_
from sqlebra.dtype import ndarray_
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
