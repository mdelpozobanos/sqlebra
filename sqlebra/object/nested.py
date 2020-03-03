from .object_ import object_
from .item import Item
from sqlebra import exceptions as ex


class NestedView:
    """View to a subset of a Nested object."""

    @property
    def id(self):
        return self.obj.id

    @property
    def db(self):
        return self.obj.db

    @property
    def py(self):
        p = self.p
        if len(p) > 0:
            return self.obj.pyclass(p_i.py for p_i in p)
        else:
            return p

    @py.setter
    def py(self):
        raise NotImplemented

    @property
    def p(self):
        """Half way through py property. List of SQLebra objects"""
        rows = self.db[{'id': self.id, 'ind': self.item, 'order_list': self.item}]
        if len(rows) > 0:
            if self.obj.pyclass is tuple:
                return rows
            else:
                return self.obj.pyclass(rows)
        else:
            return self.obj.pyclass()

    def __init__(self, obj, item):
        self.obj = obj
        self.item = item

    def __getitem__(self, item):
        if isinstance(item, list):
            return self.obj[[self.item[i] for i in item]]
        else:
            return self.obj[self.item[item]]

    def __setitem__(self, key, value):
        raise NotImplemented

    def __delitem__(self, key):
        raise NotImplemented

    def __len__(self):
        return len(self.item)


class NestedIterator:

    def __init__(self, obj):
        self.obj = obj
        self.current = -1
        # Static parameter. A dynamic parameter would be more flexible but slower. Anyway, one shouldn't
        # change the length of an iterator on the go.
        self.high = len(self.obj)

    def __iter__(self):
        return self

    def __next__(self):
        self.current += 1
        if self.current < self.high:
            obj = self.obj[self.current]
            obj.ref = Item(self.obj, ind=self.current)
            return obj
        self.current = None
        raise StopIteration


class Nested(object_):
    """
    Parent class defining an object inside an SQL database file with a multiple values
    (i.e. list, tuple or dict).
    """

    # The following variables are defined by the inheriting class.
    col_item = False  # Name of the column containing the indexing item ("key" or "ind")

    @property
    def child_id(self):
        return tuple(id[0] for id in self.db.select(self.db.tab_items, column=('child_id', ),
                                                    where={'id': self.id}, order=(self.col_item, )))

    @property
    def items(self):
        return [Item(self, ind=ind) for ind in range(len(self))]

    def __len__(self):
        return self.db.select(self.db.tab_items, column=('count(*)',), where={'id': self.id})[0][0]

    def __getitem__(self, item):
        item = self._check_item_(item)
        row = self.db.select(self.db.tab_items, column=('child_id',),
                             where={'id': self.id, self.col_item: item},
                             order=('ind',))
        if len(row) == 1:
            return self.db[row[0][0]]
        elif len(row) == 0:
            raise ex.CorruptedDatabase("{} Item {} of object {} not found".format(self.db.sqldb, item, self.id))
        elif len(row) > 1:
            raise ex.CorruptedDatabase("{} Multiple items {} in object {}".format(self.db.sqldb, item, self.id))

    def _check_item_(self, item):
        """Check the provided indexing item"""
        raise NotImplemented

    def delete(self, del_ref=True):
        # 1. If this is the only reference to the current nested SQL-object (i.e. self), clear all item objects. This
        #    is, delete all item objects if this was their only reference.
        if self.num_ref < 2:
            self.clear(del_ref=False)
        # 2. Delete items
        self.db.delete(self.db.tab_items, where={'id': self.id})
        # 3. Delete object and its reference if required
        super(Nested, self).delete(del_ref=del_ref)

    def clear(self, del_ref=True):
        # Empty nested object
        # 1. Retrieve item objects
        item_objs = self.db[self.child_id]
        # 2. Delete items to drop pointers to objs
        if del_ref:
            self.db.delete(self.db.tab_items, where={'id': self.id})
        else:
            self.db.update(self.db.tab_items, set={'child_id': None}, where={'id': self.id})
        # 3. Delete objects in items when they don't have no more references
        [obj.delete() for obj in item_objs]

    @classmethod
    def value2row(cls, x):
        """
        :param x: (IGNORED) Python value to be converted
        :return: (dict) With (key, value) = (column, value) corresponding to a row in the database
            containing the specified value.
        """
        return {'type': cls.pyclass.__name__}
