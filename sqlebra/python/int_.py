from ..object.single import Single
from ..object.numeric import Numeric
import builtins


class int_(Single, Numeric):
    """SQLobject of type integer"""

    pyclass = builtins.int
    col_val = 'int_val'
