from .object_ import object_
from sqlebra import exceptions as ex


class MultiItem:
    """Class defining SQLebra items (i.e. objects within multi_ objects). This class is entirely SQLebra
    private, as users do not normally need to deal with the distinction between items and _Items.
    """

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
        return {'id': self._ref._id, 'ind': self._ind}

    # [MultiItem; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # [MultiItem] FD
    # ==================================================================================================================
    # Methods/properties fully defined here. They can be overloaded by inheriting classes

    # [MultiItem; FD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    def __init__(self, ref, key=None, ind=None):
        """
        Parameters
        ----------
        ref: `sqlebra.object`
            SQLebra object owning the item.
        key: str
            Item's key
        ind: int
            Item's ind
        """
        self.db = ref.db
        self._ref = ref
        self._ind = ind
        self._key = key

    @property
    def py(self):
        """Item's python value"""
        return self._obj.py

    @py.setter
    def py(self, x):
        """Set item's python value"""
        if self.py == x:
            return
        new_id = self.db._objs.append(x)
        if self._id != new_id:
            self._id = self.db._objs.append(x)

    # [MultiItem; FD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    @property
    def _id(self):
        """Item's unique identifier (i.e., child_id)"""
        try:
            return self.db._items[self._where, 'child_id'][0][0]
        except ex.ItemError:
            ex.CorruptedDatabase('{} - Item {} from object {} not found in table ITEMS'.format(self.db.info,
                                                                                               self._ind,
                                                                                               self._ref._id))

    @_id.setter
    def _id(self, x):
        """Set item's unique identifier (i.e., child_id)"""
        self._obj._delete(del_ref=None)
        self.db._items[self._where] = {'child_id': x}

    @property
    def _obj(self):
        """_Item's object"""
        return self.db._objs.obj[self, self._id]

    def _delete(self):
        del self.db._items[self._where]

    # [MultiItem; FD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass


class Multi(object_):
    """
    Parent class defining an object inside an SQL database file with multiple values (e.g., list, tuple or dict).
    """

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
        raise NotImplementedError

    # [multi; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    def _item2where(self, item):
        """Validate item and return a dictionary to select it from table Items

        Parameters
        ----------
        item: (class dependant)
            Selected item(s).

        Returns
        -------
        int, tuple, slice
            Normalised selected item(s)
        dict
            Dictionary used to select the items' rows from table Items
        bool
            True if the object should return a slice.
        """
        raise NotImplemented

    @classmethod
    def _draft_obj(cls, db_draft, value):
        """
        Draft a value's rows from tables OBJECTS and ITEMS representing the value in the database, and returns its
        object identifier.

        Parameters
        ----------
        db_draft: `sqlebra.database.database.DatabaseDraft'
            Draft object of calling database
        value: `cls.pyclass`
            Python value to be drafted

        Returns
        -------
        int:
            Unique object identifier pertaining to the drafted object. If the object is new, the identifier will be < 0.

        Notes
        -----
        Multi objects are defined by multiple rows across tables OBJECTS and ITEMS. The definition of this procedure is
        fully decentralised since multi classes may follow very difference drafting procedures.

        Multi objects are not directly hashable in SQLebra. If a multi object needs to be reused, it must be passed as
        an SQLebra object to be identified as an existing object. Otherwise, a copy of the multi object will be
        allocated in the database.
        """
        raise NotImplementedError

    # [multi; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    _Multi__col_item = False  # Name of the column containing the indexing item ("key" or "ind")
    _Multi__item_class = False  # Class used for items
    _Multi__slice_class = None

    # [multi] FD
    # ==================================================================================================================
    # Methods/properties fully defined here. They can be overloaded by inheriting classes

    # [multi; FD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    def __len__(self):
        return self.db._items[{'id': self._id}, 'count(*)'][0][0]

    def __getitem__(self, item):
        # Convert item to search dictionary
        item, item_where, is_slice = self._item2where(item)
        if is_slice:
            try:
                child_ids = tuple(n[0] for n in self.db._items[item_where, 'child_id'])
            except ex.ItemError:
                raise ex.CorruptedDatabase("{} Item {} of object {} not found".format(self.db.info, item, self._id))
            return self._Multi__slice_class(ref=self, item=item, id=child_ids)
        else:
            item_ = self._Multi__item_class(self,
                                            ind=item_where.get('ind', None),
                                            key=item_where.get('key', None))
            return item_._obj

    # [multi; FD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    @property
    def _child_id(self):
        try:
            return self.db._items[{'id': self._id}, 'child_id'][0]
        except ex.ItemError as err:
            raise ex.CorruptedDatabase(self.info) from err

    def _delete(self, del_ref=True, expected_num_ref=1):
        # 1. Delete object if this is its only reference
        if expected_num_ref < 0 or self._object__num_ref <= expected_num_ref:
            # Extract items objects
            objs = self._Multi__objs_tuple
            # Delete object from tables OBJECTS and ITEMS
            del self.db._objs[{'id': self._id}]
            del self.db._items[{'id': self._id}]
            # Delete items objects
            tuple(o._delete(del_ref=False, expected_num_ref=0) for o in objs)
        # 2. Delete reference if necessary
        if del_ref and self._ref:
            self._ref._delete()

    # [multi; FD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    @property
    def _Multi__objs_tuple(self):
        """Tuple with all objects"""
        return self.db._items.obj[self, :]
