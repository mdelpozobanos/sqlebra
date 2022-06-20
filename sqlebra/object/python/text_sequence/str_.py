from collections.abc import Iterator
from sqlebra.object.single import Single
import builtins


class StrMultiIterator(Iterator):
    """An iterator class for str_"""

    def __init__(self, obj):
        super().__init__()
        self.obj = obj
        self.current = -1
        # Static parameter. A dynamic parameter would be more flexible but slower. Anyway, one shouldn't
        # change the length of an iterator on the go.
        self.high = len(self.obj)

    def __iter__(self):
        return self

    def __next__(self):
        self.current += 1
        if self.current < self.high:
            obj = self.obj[self.current]
            return obj
        self.current = None
        raise StopIteration


class str_(Single):
    """SQLobject of type str"""

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

    _value_column = 'txt_val'

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

    pyclass = builtins.str

    # [object_; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # str_
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [str_] FD
    # ==================================================================================================================
    # Methods/properties fully defined here. They can be overloaded by inheriting classes

    # [str_; FD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    def __len__(self):
        return len(self.py)

    def __getitem__(self, item):
        return self.py[item]

    def __iter__(self):
        return StrMultiIterator(self)

    # [str_; FD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    # [str_; FD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass


def method_generator(fcn):
    """Generates methods to be added to str_"""
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
