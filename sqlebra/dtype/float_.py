from ..object.single import Single
import builtins


class float_(Single):
    """SQLobject of type float"""

    pyclass = builtins.float
    col_val = 'real_val'
