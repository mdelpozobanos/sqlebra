from .. import exceptions as ex


class Item:

    @property
    def db(self):
        return self.obj.db

    @property
    def id(self):
        return self.obj.id

    @id.setter
    def id(self, value):
        """Redirect to child_id"""
        self.child_id = value

    @property
    def child_id(self):
        row = self.db.select(self.db.tab_items, column=('child_id', ), where={'id': self.id, 'ind': self.ind})
        try:
            return row[0][0]
        except IndexError:
            if len(row) == 0:
                raise ex.CorruptedDatabase('{} Multiple entries for object {} index {}'.format(
                    self.db.sqldb, self.id, self.ind))

    @child_id.setter
    def child_id(self, value):
        self.db.update(self.db.tab_items, set={'child_id': value}, where={'id': self.id, 'ind': self.ind})

    def __init__(self, obj, key=None, ind=None):
        self.obj = obj
        self.key = key
        self.ind = ind

    def delete(self):
        self.db.delete(self.db.tab_items,
                       where={'id': self.id, 'key': self.key, 'ind': self.ind})
        # If ordered object, update indices
        if self.ind is not None and self.ind < len(self.obj):
            self.db.update(self.db.tab_items, set={'*': 'ind = ind - 1'},
                           where={'id': self.id, '*': 'ind>{}'.format(self.ind)})