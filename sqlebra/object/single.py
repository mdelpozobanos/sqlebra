from .object_ import object_
from .. import exceptions as ex


class Single(object_):

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # object_
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [object_] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [object_; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    # Python values
    # -------------

    @property
    def py(self):
        """Python value of the object"""
        return getattr(self, '_' + self._value_column)  # self.__getattribute__(self.col_val)

    @py.setter
    def py(self, x):
        object_.py.fset(self, x)

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
        objs_dict = {**cls._value2row(value), 'type': cls.pyclass.__name__}
        # Check if object exists in table OBJECTS
        try:
            return db_draft._select_obj_id(objs_dict)
        except ex.ObjectError:  # -> Object does not exist. Draft a new object
            id = db_draft._free_id()[0]  # -> new id
            objs_dict['id'] = id
            db_draft._objs.append(objs_dict)
            return id

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # single
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [single] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    pass

    # [single; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pass

    # [single; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    _value_column = False  # Name of the column in objects table containing the value (int_val, txt_val, etc)

    # [single; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # [single] FD
    # ==================================================================================================================
    # Methods/properties fully defined here. They can be overloaded by inheriting classes

    # [single; FD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    # Casting:

    def __int__(self):
        return int(self.py)

    def __float__(self):
        return float(self.py)

    # [single; FD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    @classmethod
    def _value2row(cls, x):
        """Return dict with the value dependant keys/columns representing the variable `x` in the database.

        Parameters
        ----------
        x: `cls.pyclass`
            Python value to be converted

        Return
        ------
        dict
            A dict with (key, value) = (column, value) with the value dependant part of a row in the database containing
            `x`.
        """
        return {cls._value_column: x}

    # [single; FD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass
