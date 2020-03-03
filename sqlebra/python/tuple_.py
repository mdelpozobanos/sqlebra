from ..object.nested import NestedIterator, NestedView
from .tuple_list_ import tuple_list_
import builtins


class tupleView(NestedView):
    """View to a subset of an SQLebra tuple."""

    @property
    def py(self):
        return NestedView.py.fget(self)

    @py.setter
    def py(self):
        raise TypeError("'{}' is immutable".format(self.sqlclass))

    def __setitem__(self, key, value):
        raise TypeError("{} is immutable".format(self.sqlclass))

    def __delitem__(self, key):
        raise TypeError("{} is immutable".format(self.sqlclass))


class tupleIterator(NestedIterator):
    pass


class tuple_(tuple_list_):

    pyclass = builtins.tuple
    col_item = 'ind'
    _slice_class = tupleView
    _iterator_class = tupleIterator

    def __setitem__(self, key, value):
        raise TypeError("{} is immutable".format(self.sqlclass))

    def __delitem__(self, key):
        raise TypeError("{} is immutable".format(self.sqlclass))
