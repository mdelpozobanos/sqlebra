from sqlebra.object.object_ import object_
from sqlebra.object.single import Single


class _Numeric(Single):
    """Class defining numeric properties and methods for SQLebra objects"""

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # Numeric
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [Numeric] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [Numeric; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pass

    # [Numeric; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [Numeric; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # [Numeric] FD
    # ==================================================================================================================
    # Methods/properties fully defined here. They can be overloaded by inheriting classes

    # [Numeric; FD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    # Properties
    # ------------------

    @property
    def denominator(self):
        return self.py.denominator

    @property
    def imag(self):
        return self.py.imag

    @property
    def numerator(self):
        return self.py.numerator

    @property
    def real(self):
        return self.py.real

    # Binary operators
    # ----------------

    def __add__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.py + other

    def __radd__(self, other):
        if isinstance(other, object_):
            other = other.py
        return other + self.py

    def __sub__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.py - other

    def __rsub__(self, other):
        if isinstance(other, object_):
            other = other.py
        return other - self.py

    def __mul__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.py * other

    def __rmul__(self, other):
        if isinstance(other, object_):
            other = other.py
        return other * self.py

    def __truediv__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.py / other

    def __rtruediv__(self, other):
        if isinstance(other, object_):
            other = other.py
        return other / self.py

    def __floordiv__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.py // other

    def __rfloordiv__(self, other):
        if isinstance(other, object_):
            other = other.py
        return other // self.py

    def __mod__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.py % other

    def __rmod__(self, other):
        if isinstance(other, object_):
            other = other.py
        return other % self.py

    def __pow__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.py ** other

    def __rpow__(self, other):
        if isinstance(other, object_):
            other = other.py
        return other ** self.py

    # Comparison operators
    # --------------------

    def __lt__(self, other):
        if isinstance(other, object_):
            if id(self) == id(other):
                return False
            elif self._id == other._id:
                return False
            else:
                return self.py < other.py
        else:
            return self.py < other

    def __gt__(self, other):
        if isinstance(other, object_):
            if id(self) == id(other):
                return False
            elif self._id == other._id:
                return False
            else:
                return self.py > other.py
        else:
            return self.py > other

    def __le__(self, other):
        if isinstance(other, object_):
            if id(self) == id(other):
                return True
            elif self._id == other._id:
                return True
            else:
                return self.py <= other.py
        else:
            return self.py <= other

    def __ge__(self, other):
        if isinstance(other, object_):
            if id(self) == id(other):
                return True
            elif self._id == other._id:
                return True
            else:
                return self.py >= other.py
        else:
            return self.py >= other

    # def __eq__(self, other): # defined in object.py
    # def __ne__(self, other):  # defined in object.py

    # In-place operators
    # ------------------

    def __iadd__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self._numeric__inplace(self.py + other)

    def __isub__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self._numeric__inplace(self.py - other)

    def __imul__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self._numeric__inplace(self.py * other)

    def __itruediv__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self._numeric__inplace(self.py / other)

    def __ifloordiv__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self._numeric__inplace(self.py // other)

    def __imod__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self._numeric__inplace(self.py % other)

    def __ipow__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self._numeric__inplace(self.py ** other)

    # Unary operators
    # ---------------

    def __neg__(self):
        return -self.py

    def __pos__(self):
        return +self.py

    def __invert__(self):
        return ~self.py

    # [Numeric; FD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [Numeric; FD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    def _numeric__inplace(self, x):
        self._ref.py = x
        return self._ref._obj


def method_generator(fcn):
    """Generates methods to be added to `numeric`"""
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
for method in ('bit_length', 'conjugate', 'from_bytes', 'to_bytes', 'as_integer_ratio'):
    cls_method = method_generator(method)
    setattr(_Numeric, cls_method.__name__, cls_method)


# TODO: Explore the possibility of doing "def property_generator(property):"
# for property in ('denominator', 'imag', 'numerator', 'real', ):
