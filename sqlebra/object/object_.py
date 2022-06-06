from sqlebra import SQLebraObject


class object_(SQLebraObject):
    """
    Object defined in the SQL database.

    This class is defined so that all classes inheriting from this can mutate in the fly (e.g., changing from int to
    float).
    """

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # object_
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [object_] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [object_; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pyclass = NotImplementedError  # Python class

    # [object_; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

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
        """
        raise NotImplementedError

    # [object_; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # [object_] FD
    # ==================================================================================================================
    # Methods/properties fully defined here. They can be overloaded by inheriting classes

    # [object_; FD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    def __init__(self, db, id, ref):
        """
        Parameters
        ----------
        db: SQLebra.Database
            SQLebra database containing the object
        id: int
            Unique object identifier
        ref: SQLebra.Variable or SQLebra.Item
            Variable or item referencing this object
        """
        self.db = db
        self._id_ = id  # saved in `_id_` and accessed to through `_id`
        self._ref = ref  # variable/object referencing this object

    @property
    def info(self):
        """SQL related information"""
        return '{}[sqlebra.{}:id{}]'.format(self.db.info, self.pyclass.__name__, self._id)

    def delete(self):
        """Delete an object and its reference from the database"""
        self._delete()

    # Python values
    # -------------

    @property
    def py(self):
        """Python value of the object"""
        raise NotImplementedError

    @py.setter
    def py(self, x):
        self._ref.py = x
        # TODO: Can this be done better? Maybe self._id should return self._ref._id? Maybe not, self is an object and
        #  therefore its id is immutable. Changing self._id is akin to mutate self. So maybe 'yes', and by making
        #  self.id = self._ref._id we make the object be dependant of the reference and the user's point of view
        #  WARNING! This is very dodgy... How should we support a mutating variable? (Explore __new__?)
        if self._id != self._ref._id:
            self._id = self._ref._id
            new_self = self._ref._obj
            if not isinstance(new_self, type(self)):
                self.__class__ = self._ref._obj.__class__
                # x = self.__new__(self._ref._obj.__class__, db=self.db, id=self._id, ref=self._ref)

    # Operators
    # ---------

    def __eq__(self, other):
        if isinstance(other, object_):
            if id(self) == id(other):
                return True
            elif self._id == other._id:
                return True
            else:
                return self.py == other.py
        else:
            return self.py == other

    def __ne__(self, other):
        if isinstance(other, object_):
            if id(self) == id(other):
                return False
            elif self._id == other._id:
                return False
            else:
                return self.py != other.py
        else:
            return self.py != other

    def isNone(self):
        """Convenient method to avoid doing x != None"""
        # TODO: Seems slow
        from sqlebra.object.python import NoneType_
        return isinstance(self, NoneType_)

    # casting
    # -------

    def __str__(self):
        return str(self.py)

    def __bool__(self):
        return bool(self.py)

    # Database direct access
    # ----------------------

    @property
    def __enter__(self):
        """Return a transaction of the associated database"""
        return self.db.__enter__

    @property
    def __exit__(self):
        """Return a transaction of the associated database"""
        return self.db.__exit__

    # [object_; FD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    @property
    def _id(self):
        """Object's unique identifier"""
        return self._id_

    @_id.setter
    def _id(self, value):
        self._id_ = value
        if self._ref:
            self._ref.id = value

    @property
    def _bool_val(self):
        """Object's bool_val value in the database's objects table"""
        return bool(self.db._objs[{'id': self._id}, 'bool_val'][0][0])

    @property
    def _int_val(self):
        """Object's int_val value in the database's objects table"""
        return self.db._objs[{'id': self._id}, 'int_val'][0][0]

    @property
    def _real_val(self):
        """Object's real_val value in the database's objects table"""
        return self.db._objs[{'id': self._id}, 'real_val'][0][0]

    @property
    def _txt_val(self):
        """Object's txt_val value in the database's objects table"""
        return self.db._objs[{'id': self._id}, 'txt_val'][0][0]

    def _delete(self, del_ref=True, expected_num_ref=1):
        """Delete object from the database

        Parameters
        ----------
        del_ref: bool
            If True, the object's reference (`_ref`) will also be deleted.
        expected_num_ref: int
            When the number of expected references is lower than the number of actual references in the database, the
            object is referenced by other objects and therefore it cannot be deleted. Having objects with multiple
            references in the database is a way of saving memory.
        """
        # 1. Delete object if this is its only reference
        if expected_num_ref < 0 or self._object__num_ref <= expected_num_ref:
            del self.db._objs[{'id': self._id}]
        # 2. Delete reference if necessary
        if del_ref and self._ref:
            self._ref._delete()

    # [object_; FD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    @property
    def _object__num_ref(self):
        """Number of references the object has in the database"""
        # TODO: Move to self.db._objs? Maybe not, note how this is overloaded by database type
        return self.db._obj_ref_count(id=self._id)

    def _object__exist(self):
        """Return True if the object exists in the SQL database"""
        return {'id': self._id} in self.db._objs
