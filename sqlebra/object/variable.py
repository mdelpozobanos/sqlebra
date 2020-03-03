class Variable:

    @property
    def row(self):
        return self.db.select(self.db.tab_vars, where={'name': self.name})

    @property
    def id(self):
        return self.db.select(self.db.tab_vars,
                              column=('id',),
                              where={'name': self.name})[0][0]

    @id.setter
    def id(self, x):
        """
        If this variable is the only one referencing the current SQL object (id), it must be deleted before updating
        the pointer
        """
        # if self.obj.num_ref == 1:  # The old object is not used anymore. Delete
        #     self.db.delete(self.db.objs_tab, where={'id': self.id})
        self.db.update(self.db.tab_vars, set={'id': x}, where={'name': self.name})

    @property
    def obj(self):
        obj = self.db[self.id]
        obj.ref = self
        return obj

    def __init__(self, db, name):
        self.db = db
        self.name = name

    def exist(self):
        return self.db.select(self.db.tab_vars, column=('count(*)',), where={'name': self.name})[0][0] == 1

    def delete(self):
        self.db.delete(self.db.tab_vars, where={'name': self.name})
