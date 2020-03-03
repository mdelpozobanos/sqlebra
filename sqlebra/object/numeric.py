from .object_ import object_


class Numeric:

    # Properties

    @property
    def denominator(self):
        return self.denominator

    @property
    def imag(self):
        return self.imag

    @property
    def numerator(self):
        return self.numerator

    @property
    def real(self):
        return self.real

    # Binary operators

    def __add__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.py + other

    def __sub__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.py - other

    def __mul__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.py * other

    def __truediv__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.py / other

    def __floordiv__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.py // other

    def __mod__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.py % other

    def __pow__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.py ** other

    # Comparison operators

    def __lt__(self, other):
        if isinstance(other, object_):
            if id(self) == id(other):
                return False
            elif self.id == other.id:
                return False
            else:
                return self.py < other.py
        else:
            return self.py < other

    def __gt__(self, other):
        if isinstance(other, object_):
            if id(self) == id(other):
                return False
            elif self.id == other.id:
                return False
            else:
                return self.py > other.py
        else:
            return self.py > other

    def __le__(self, other):
        if isinstance(other, object_):
            if id(self) == id(other):
                return True
            elif self.id == other.id:
                return True
            else:
                return self.py <= other.py
        else:
            return self.py <= other

    def __ge__(self, other):
        if isinstance(other, object_):
            if id(self) == id(other):
                return True
            elif self.id == other.id:
                return True
            else:
                return self.py >= other.py
        else:
            return self.py >= other

    # def __eq__(self, other): # defined in object.py
    # def __ne__(self, other):  # defined in object.py

    # In-place operators

    def __inplace__(self, x):
        if isinstance(x, self.pyclass):
            self.py = x
            return self
        else:
            # Delete reference
            self.delete(del_ref=False)
            # Attach reference to a new id
            id = self.db.free_id()[0]
            self.ref.id = self.db.free_id()
            # Create object with result using the new id
            self.db[id] = x
            # Retrieve id object and link it to the reference
            x = self.db[id]
            x.ref = self.ref
            # Return
            return x

    def __iadd__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.__inplace__(self.py + other)

    def __isub__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.__inplace__(self.py - other)

    def __imul__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.__inplace__(self.py * other)

    def __itruediv__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.__inplace__(self.py / other)

    def __ifloordiv__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.__inplace__(self.py // other)

    def __imod__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.__inplace__(self.py % other)

    def __ipow__(self, other):
        if isinstance(other, object_):
            other = other.py
        return self.__inplace__(self.py ** other)

    # Unary operators

    def __neg__(self):
        return -self.py

    def __pos__(self):
        return +self.py

    def __invert__(self):
        return ~self.py


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
for method in ('bit_length', 'conjugate', 'from_bytes', 'to_bytes', 'as_integer_ratio'):
    cls_method = method_generator(method)
    setattr(Numeric, cls_method.__name__, cls_method)
