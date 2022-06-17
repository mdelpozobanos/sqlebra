class _Variable:
    """Class defining SQLebra variables (i.e. highest level objects in the database). This class is entirely SQLebra
    private, as users do not normally need to deal with the distinction between objects and variables.
    """

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # Variable
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [Variable] FD
    # ==================================================================================================================
    # Methods/properties fully defined here. They can be overloaded by inheriting classes

    # [Variable; FD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    def __init__(self, db, name):
        """
        Parameters
        ----------
        db: sqlebra.database.Database
            Database holding the variable.
        name: str
            Name of the variables as stated in the variables table.
        """
        if not name in db:
            raise NameError("Variable '{}' not found in {}".format(name, db.file))
            # TODO need to decide on error types in SQLebra
        self.db = db
        self.name = name
        self._ref = None

    @property
    def py(self):
        """Variable's python value"""
        return self._obj.py

    @py.setter
    def py(self, x):
        """Set variable's python value"""
        self.db[self.name] = x

    # [Variable; FD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    @property
    def _id(self):
        """Variable's unique identifier"""
        return self.db._vars[{'name': self.name}, 'id'][0][0]

    @_id.setter
    def _id(self, x):
        """Set variable's unique identifier"""
        self.db._vars[{'name': self.name}] = {'id': x}

    @property
    def _obj(self):
        """Variable's object"""
        return self.db._objs.obj[self, self._id]

    def _exist(self):
        """Return true if the variable exist in the database"""
        return self.name in self.db

    def _delete(self):
        """Delete variable"""
        del self.db._vars[{'name': self.name}]

    # [Variable; FD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass
