from ..object.single import Single
from ..object.nested import NestedIterator
import builtins


class str_iterator(NestedIterator):

    pass


class str_(Single):
    """SQLobject of type str"""

    pyclass = builtins.str
    col_val = 'txt_val'
    _iterator_class = str_iterator

    def __len__(self):
        return len(self.py)

    def __getitem__(self, item):
        return self.py[item]

    def __iter__(self):
        return self._iterator_class(self)


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
for method in ('capitalize', 'casefold', 'center', 'count', 'encode', 'endswith', 'expandtabs', 'find',
               'format', 'format_map', 'index', 'isalnum', 'isalpha', 'isdecimal', 'isdigit', 'isidentifier',
               'islower', 'isnumeric', 'isprintable', 'isspace', 'istitle', 'isupper', 'join', 'ljust',
               'lower', 'lstrip', 'maketrans', 'partition', 'replace', 'rfind', 'rindex', 'rjust', 'rpartition',
               'rsplit', 'split', 'splitlines', 'startswith', 'strip', 'swapcase', 'title', 'translate',
               'upper', 'zfill'):
    cls_method = method_generator(method)
    setattr(str_, cls_method.__name__, cls_method)
