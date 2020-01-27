from sqlebra.dtype import list_
import builtins


class tuple_(list_):

    pyclass = builtins.tuple

    def __setitem__(self, key, value):
        raise TypeError("'tuple' object does not support item assign")

    @property
    def py(self):
        return builtins.tuple(list_.py.fget(self))

    @py.setter
    def py(self, x):
        return list_.py.fset(self, x)

    def __setitem__(self, key, value):
        raise TypeError("'{}' object does not support item assignment".format(type(self)))
