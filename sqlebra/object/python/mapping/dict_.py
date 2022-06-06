from sqlebra.object.multi import MultiItem, Multi
import builtins
from sqlebra import exceptions as ex
from sqlebra.object.object_ import object_


class dictItem(MultiItem):

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # MultiItem
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [MultiItem] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [MultiItem; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pass

    # [MultiItem; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    @property
    def _where(self):
        """Dictionary used as the `where` argument to select the item's row in table ITEMS"""
        return {'id': self._ref._id, 'key': self._key}

    # [MultiItem; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass


class dict_keys:
    """View of dict keys"""

    def __init__(self, x):
        self.x = x
        self.xkeys = tuple(n[0] for n in self.x.db._items[{'id': self.x._id}, 'key'])
        self.current = -1
        self.high = len(self.x)

    def __iter__(self):
        return self

    def __next__(self):
        self.current += 1
        if self.current < self.high:
            return self.xkeys[self.current]
        raise StopIteration


class dict_values:
    """View of dict values"""

    def __init__(self, x):
        self.x = x
        self.xkeys = tuple(n[0] for n in self.x.db._items[{'id': self.x._id}, 'key'])
        self.current = -1
        self.high = len(self.x)

    def __iter__(self):
        return self

    def __next__(self):
        self.current += 1
        if self.current < self.high:
            try:
                return self.x[self.xkeys[self.current]]
            except:
                return self.x[self.xkeys[self.current]]
        raise StopIteration


class dict_items:
    """View of dict items"""

    def __init__(self, x):
        self.x = x
        self.xkeys = tuple(n[0] for n in self.x.db._items[{'id': self.x._id}, 'key'])
        self.current = -1
        self.high = len(self.x)

    def __iter__(self):
        return self

    def __next__(self):
        self.current += 1
        if self.current < self.high:
            key = self.xkeys[self.current]
            return key, self.x[key]
        raise StopIteration


class dict_(Multi):
    """SQL object of type dict"""

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # object_
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [object_] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [object_; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pyclass = builtins.dict  # Python class

    # Python values
    # -------------

    @property
    def py(self):
        """Python value of the object"""
        objs = self.objs
        if len(objs) > 0:
            return self.pyclass((k, o.py) for k, o in objs.items())
        else:
            return objs

    @py.setter
    def py(self, x):
        object_.py.fset(self, x)

    # [object_; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    # def _draft_obj(cls, db_draft, value):

    # [object_; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # multi
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [multi] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [multi; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    @property
    def objs(self):
        """Half-way-through py property. Object of type `self.pyclass` with values in SQLebra object form"""
        key_item_id = self.db._items[{'id': self._id}, ('key', 'child_id',)]
        if len(key_item_id) > 0:
            # Extract keys
            key = tuple(map(lambda n: n[0], key_item_id))
            # Extract objects
            try:
                objs = self.db._objs.obj[self, tuple(map(lambda n: n[1], key_item_id))]
            except ex.ObjectError:
                raise ex.CorruptedDatabase('Object id {} is corrupted'.format(self._id))
            # Return object
            return self.pyclass(zip(key, objs))
        else:
            return self.pyclass()

    # [multi; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    @classmethod
    def _draft_obj(cls, db_draft, value, _id=None):
        """
        Draft a value's rows from tables OBJECTS and ITEMS representing the value in the database, and returns its
        object identifier.

        Parameters
        ----------
        db_draft: `sqlebra.database.database.DatabaseDraft'
            Draft object of calling database
        value: `cls.pyclass`
            Python value to be drafted
        _id: int
            Object identifier. If not specified (default) a new object will be drafted. If specified, only items will be
            drafted.

        Returns
        -------
        int:
            Unique object identifier pertaining to the drafted object. If the object is new, the identifier will be < 0.

        Notes
        -----
        Multi objects are defined by multiple rows across tables OBJECTS and ITEMS. The definition of this procedure is
        fully decentralised since multi classes may follow very difference drafting procedures.

        Sequence objects are not directly hashable in SQLebra. If a sequence object needs to be reused, it must be
        passed as an SQLebra object to be identified as an existing object. Otherwise, a copy of the sequence object
        will be allocated in the database.
        """
        if _id is None:  # Draft new object
            objs_dict = {'type': cls.pyclass.__name__}
            _id = db_draft._free_id()[0]
            objs_dict['id'] = _id
            db_draft._objs.append(objs_dict)
        items_dict = {'key': tuple(value.keys())}
        items_dict['id'] = (_id,) * len(items_dict.get('key'))
        # Go through items, drafting and retrieving their id
        items_dict['child_id'] = []
        for v in items_dict['key']:  # v = items_dict['key'][0]
            v_id = db_draft._draft_obj(value[v])
            items_dict['child_id'].append(v_id)
        db_draft._items.append(items_dict)
        return _id

    def _item2where(self, item):
        """Validate item and return a dictionary to select it from table Items

        Parameters
        ----------
        item: str
            Selected item(s) (key).

        Returns
        -------
        dict
            Dictionary used to select the items' rows from table Items
        """
        if item in self:
            return item, {'id': self._id, 'key': item}, False
        else:
            raise KeyError('{}'.format(item))

    # [multi; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    _Multi__col_item = 'key'  # Name of the column containing the indexing item ("key" or "ind")
    _Multi__item_class = dictItem

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # dict_
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [dict_] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [dict_; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pass

    # [dict_; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [dict_; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # [dict_] FD
    # ==================================================================================================================
    # Methods/properties fully defined here. They can be overloaded by inheriting classes

    # [dict_; FD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    def __setitem__(self, item, value):
        if item in self:
            row = self[item]
            row._ref.py = value
        else:
            self._draft_obj(self.db._draft, {item: value}, _id=self._id)
            self.db._draft._commit()

    def __delitem__(self, item):
        row = self[item]
        row._delete()

    def __contains__(self, item):
        if not isinstance(item, str):
            try:
                item = str(item)
            except:
                raise TypeError("'{}' only supports {} keys. Found '{}' instead".format(type(self), str, type(item)))
        return self.db._items[{'id': self._id, 'key': item}, 'count(*)'][0][0] == 1

    def __iter__(self):
        return dict_keys(self)

    def pop(self, key, *args):
        key, _, _ = self._item2where(key)
        # Retrieve value
        x = self[key]
        x_py = x.py
        # Delete variable and value from list
        x._delete()
        return x_py

    def keys(self):
        return dict_keys(self)

    def values(self):
        return dict_values(self)

    def items(self):
        return dict_items(self)

    # [dict_; FD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [dict_; FD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass
