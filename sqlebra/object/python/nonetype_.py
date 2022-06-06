from sqlebra.object.single import Single


class NoneType_(Single):
    """SQLebra object of type None"""

    pyclass = None.__class__
    _value_column = None

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # object_
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [object_] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [object_; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    @property
    def py(self):
        return None

    @py.setter
    def py(self, x):
        super(NoneType_, self.__class__).py.fset(self, x)

    # [object_; TBD] Private interface
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
        return {}

