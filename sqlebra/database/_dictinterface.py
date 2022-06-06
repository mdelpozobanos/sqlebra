class _DictInterface:
    """Adds a dictionary like user/public interface to the database"""

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # _DictInterface
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [_DictInterface] FD
    # ==================================================================================================================
    # Methods/properties fully defined here. They can be overloaded by inheriting classes

    # [_DictInterface; FD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    def __setitem__(self, name, value):
        """Add variable to the database or set value of existing variable

        Parameters
        ----------
        name: str
            Variable name
        value:
        """
        self._vars.obj[name] = value

    def __getitem__(self, name):
        """Return the selected variable from table Variables.

        Parameters
        ----------
        name: str or int
            Variable name (str) or identifier (int)

        Returns
        -------
        sqlebra.object:
            Selected variable
        """
        return self._vars.obj[name]

    def __contains__(self, name):
        """Return true if the specified variable exist in the database

        Parameters
        ----------
        name: str
            Variable name.

        Returns
        -------
        bool:
            True if variable name exists in table VARIABLES.
        """
        return name in self._vars

    def __len__(self):
        """Number of variables in the database"""
        return len(self._vars)

    def __eq__(self, other):
        """Compare SQLebra databases or an SQLebra database with a dictionary"""
        if isinstance(other, self.__class__):
            if id(self) == id(other):
                return True
            elif self.file == other.file and self.name == other.name:
                return True
            else:
                return False
        elif isinstance(other, dict):
            return self.py == other
        else:
            return TypeError('Cannot compared {} with {}'.format(type(self), type(other)))

    def __delitem__(self, key):
        self._vars.obj[key].delete()

    # [_DictInterface; FD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [_DictInterface; FD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass
