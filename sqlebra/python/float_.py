from ..object.single import Single
from ..object.numeric import Numeric
import builtins


class float_(Single, Numeric):
    """SQLobject of type float"""

    pyclass = builtins.float
    col_val = 'real_val'


def method_generator(fcn):
    """Generates methods to be added to tuple_list_"""
    if isinstance(fcn, str):
        def method(self, *args, **kwargs):
            return self.py.__getattribute__(fcn)(*args, **kwargs)
        method.__name__ = fcn
    else:
        def method(self, *args, **kwargs):
            return fcn(self.py, *args, **kwargs)
        method.__name__ = fcn.__name__
    return method


# Add methods that are not supported natively
for method in ('fromhex', 'hex', 'is_integer'):
    cls_method = method_generator(method)
    setattr(Numeric, cls_method.__name__, cls_method)
