from .ndarray_ import ndarray_

# Add numpy to SQLebra

from sqlebra import py2sql

py2sql.py2sql[ndarray_.pyclass] = ndarray_
py2sql.py2sql_str[ndarray_.pyclass.__name__] = ndarray_
