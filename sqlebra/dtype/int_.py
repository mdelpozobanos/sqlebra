from ..object.single import Single
import builtins


class int_(Single):
    """SQLobject of type integer"""

    pyclass = builtins.int
    col_val = 'int_val'
