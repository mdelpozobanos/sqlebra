from . import Numeric
import builtins
import math


class float_(Numeric):
    """SQLobject of type float"""

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # Single
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [Single] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [Single; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pass

    # [Single; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    _value_column = 'real_val'

    # [Single; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # object_
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [object_] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [object_; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pass

    # [object_; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pyclass = builtins.float

    # [object_; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # object_
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # NOTE: The properties and methods below are overloaded to support '+inf', '-inf' and 'nan' floats

    # [object_] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [object_; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    @property
    def py(self):
        # Try the normal procedures
        x = super(float_, self.__class__).py.fget(self)
        # If the value is empty (None), try the text field
        if x is None:
            return float(self._txt_val)
        else:
            return x

    @py.setter
    def py(self, x):
        super(float_, self.__class__).py.fset(self, x)

    # [object_; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    @classmethod
    def _value2row(cls, x):
        if math.isinf(x):
            if x > 0:
                return {'txt_val': '+inf'}
            else:
                return {'txt_val': '-inf'}
        elif math.isnan(x):
            return {'txt_val': 'nan'}
        else:
            return super(float_, cls)._value2row(x)


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
    setattr(float_, cls_method.__name__, cls_method)
