from sqlebra import issqlebra
from sqlebra.exceptions import VariableError


class None_:
    pass


class TrackedDict(dict):

    def __init__(self, *args, **kwargs):
        """
        SQLebra dict with changes staged in memory until committed

        :param db___: (tuple) (<SQLebra database variable>,<variable name>), or an SQLebra dict. The name `db___` was
            used as an unlikely dictionary key to avoid collisions.
        """
        try:
            dbobj = kwargs.pop('db___', False)
            if isinstance(dbobj, tuple):  # -> (<SQLebra database variable>,<variable name>)
                try:
                    self._dbobj = dbobj[0][dbobj[1]]
                except VariableError:  # -> New dictionary
                    dbobj[0][dbobj[1]] = {}
                    self._dbobj = dbobj[0][dbobj[1]]
            else:  # -> <SQLebra variable> assumed
                self._dbobj = dbobj
        except KeyError:
            raise VariableError("TrackedDict expects argument 'db___'.")
        super(TrackedDict, self).__init__(*args, **kwargs)

        # dict tracking edited keys
        self._editted = {}
        for k in super(TrackedDict, self).keys():
            self._editted[k] = True

        # List of keys pinned permanently to memory
        self._pinned = []

    # dict methods
    # -------------------------------------------

    def clear(self):
        super(TrackedDict, self).clear()
        self._dbobj.clear()
        self._editted = {}

    def copy(self):
        return TrackedDict(db___=self._dbobj, **self)

    def get(self, k, d=None):
        try:
            return self[k]  # delegate looking in dbfile to __getitem__
        except KeyError:
            return d

    def items(self):
        for i in super(TrackedDict, self).items():
            yield i
        # Check non-staged and non-popped items
        for i in self._dbobj.items():
            if not super(TrackedDict, self).__contains__(i[0]) and i[0] not in self._editted:
                yield i

    def keys(self):
        for k in super(TrackedDict, self).keys():
            yield k
        # Check non-staged and non-popped keys
        for k in self._dbobj.keys():
            if not super(TrackedDict, self).__contains__(k) and k not in self._editted:
                yield k

    def pop(self, k, *args):
        if len(args) > 1:
            raise TypeError('pop expected at most 2 arguments, got {}'.format(len(args)+1))
        if k in self._pinned:
            return self._dbobj.pop(k, *args)
        try:
            try:
                r = super(TrackedDict, self).pop(k, *args)
            except KeyError:
                if k in self._editted:
                    # key not in py dict but in tracked changes -> must have been popped
                    raise
                # Try on memory
                r = self._dbobj[k]
                if issqlebra(r):
                    r = r.py
        except KeyError:  # -> Key not found
            if len(args) == 0:
                raise
            else:
                return args[0]
        else:  # -> Key found
            self._editted[k] = None_
            return r

    def popitem(self):
        raise NotImplementedError

    def setdefault(self, k, d=None):
        raise NotImplementedError

    def update(self, E=None, **F):
        raise NotImplementedError

    def values(self):
        for v in super(TrackedDict, self).values():
            yield v
        # Check non-staged and non-popped values
        for k in self._dbobj.keys():
            if not super(TrackedDict, self).__contains__(k) and k not in self._editted:
                # Load value into py dict
                v = self._dbobj[k]
                if issqlebra(v):
                    v = v.py
                super(TrackedDict, self).__setitem__(k, v)
                self._editted[k] = False
                yield v

    def __contains__(self, *args, **kwargs):
        if super(TrackedDict, self).__contains__(*args, **kwargs):
            return True
        elif self._editted.__contains__(*args, **kwargs):
            # key not in py dict but in tracked changes -> must have been popped
            return False
        else:
            return self._dbobj.__contains__(*args, **kwargs)

    def __delitem__(self, k):
        if k in self._pinned:
            self._dbobj.__delitem__(k)
        try:
            super(TrackedDict, self).__delitem__(k)
        except KeyError:
            if self._editted.__delitem__(k):  # -> must have been popped
                raise
            elif k in self._dbobj:  # -> track change
                self._editted[k] = None_
            else:  # -> key not found
                raise
        else:
            self._editted[k] = None_

    def __eq__(self, x):
        try:
            for k, v in self.items():
                if x[k] != v:
                    return False
        except KeyError:
            return False
        return True

    # def __getattribute__(self, *args, **kwargs):

    def __getitem__(self, k):
        try:
            return super(TrackedDict, self).__getitem__(k)
        except KeyError:
            if k in self._editted:
                # key not in py dict but in tracked changes -> must have been popped
                raise
            val = self._dbobj[k]
            if issqlebra(val):
                val = val.py

            # Move key to py dict
            super(TrackedDict, self).__setitem__(k, val)
            self._editted[k] = False
            return val

    def __ge__(self, *args, **kwargs):
        raise NotImplementedError

    def __gt__(self, *args, **kwargs):
        raise NotImplementedError

    def __len__(self, *args, **kwargs):
        # delegate to keys
        return len(tuple(None for _ in self.keys()))

    def __le__(self, *args, **kwargs):
        raise NotImplementedError

    def __lt__(self, *args, **kwargs):
        raise NotImplementedError

    def __ne__(self, *args, **kwargs):
        raise NotImplementedError

    def __repr__(self, *args, **kwargs):
        r_mem = []
        for k, v in self._dbobj.items():
            if not (super(TrackedDict, self).__contains__(k) or k in self._editted):
                if issqlebra(v):
                    v = v.py
                r_mem.append("'{}': {}".format(k, v))
        if len(r_mem) == 0:
            return super(TrackedDict, self).__repr__()
        elif super(TrackedDict, self).__len__() == 0:
            return '{' + ', '.join(r_mem) + '}'
        else:
            return super(TrackedDict, self).__repr__()[:-1] + ', ' + ', '.join(r_mem) + '}'

    def __setitem__(self, k, v):
        if k in self._pinned:
            self._dbobj[k] = v
        else:
            super(TrackedDict, self).__setitem__(k, v)
            self._editted[k] = True

    def __sizeof__(self):
        raise NotImplementedError

    # Special methods
    # -------------------------------------------

    def pin(self, k):
        """Pin a key permanently to memory"""
        if k not in self._pinned:
            self._pinned.append(k)
        if super(TrackedDict, self).__contains__(k):  # -> commit and flush key
            self.commit(k)
            self.flush(k)

    def unpin(self, k):
        """Unpin a key from memory"""
        if k in self._pinned:
            self._pinned.pop(k)

    def flush(self, *args):
        """Remove from py dict all committed keys"""
        if len(args) == 0:
            args = super(TrackedDict, self).keys()
        for k in args:
            if k not in self._editted:
                super(TrackedDict, self).pop(k)

    def delete(self):
        """Delete database object"""
        self._dbobj.delete()

    # Tracking methods
    # -------------------------------------------

    def rollback(self, k=None):
        """Remove all staged changes going back to the last saved stage"""
        if k is None:
            super(TrackedDict, self).clear()
            self._editted = {}
        else:
            for kn in k:
                super(TrackedDict, self).pop(kn)
                self._editted.pop(kn)

    def commit(self, *args):
        """Save all staged changes"""
        if len(args) == 0:
            args = tuple(k for k in self._editted.keys())
        with self._dbobj:
            for k in args:
                if k not in self:
                    raise KeyError(k)
                e = self._editted.pop(k, False)
                if not e:  # -> nothing to commit
                    continue
                elif e is None_:  # -> popped key
                    del self._dbobj[k]
                else:
                    self._dbobj[k] = super(TrackedDict, self).__getitem__(k)
                    # self._dbobj__[k] = super(TrackedDict, self).pop(k)
