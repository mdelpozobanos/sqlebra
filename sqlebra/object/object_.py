from .item import Item
from .. import exceptions as ex


class object_:
    """Object defined in the SQL database"""

    _pyclass = False  # Python class

    @property
    def pyclass(self):
        if self.type == self._pyclass.__name__:
            return self._pyclass
        else:
            a = 10
            raise NotImplemented

    @property
    def sqlclass(self):
        return 'sqlebra.{}'.format(self.__class__.__name__)

    @property
    def sql_info(self):
        return '{}[{}:id{}]'.format(self.db.sqldb, self.sqlclass, self.id)

    @property
    def row(self):
        return self.db.select(self.db.tab_objs, where={'id': self.id})

    @property
    def type(self):
        return self.db.select(self.db.tab_objs,
                              column=('type',),
                              where={'id': self.id})[0][0]

    @property
    def bool_val(self):
        return self.db.select(self.db.tab_objs,
                              column=('bool_val',),
                              where={'id': self.id})[0][0] == 1

    @property
    def int_val(self):
        return self.db.select(self.db.tab_objs,
                              column=('int_val',),
                              where={'id': self.id})[0][0]

    @property
    def real_val(self):
        res = self.db.select(self.db.tab_objs,
                             column=('real_val',),
                             where={'id': self.id})
        try:
            return res[0][0]
        except IndexError:
            raise ex.CorruptedDatabase('Object {} not found in objects table'.format(self.id))

    @property
    def txt_val(self):
        return self.db.select(self.db.tab_objs,
                              column=('txt_val',),
                              where={'id': self.id})[0][0]

    @property
    def py(self):
        raise NotImplemented

    @property
    def num_ref(self):
        return self.db.select(self.db.tab_vars, column=('count(*)',), where={'id': self.id})[0][0] + \
               self.db.select(self.db.tab_items, column=('count(*)',), where={'child_id': self.id})[0][0]

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value
        if self.ref:
            if isinstance(self.ref, Item):
                self.ref.child_id = value
            else:
                self.ref.id = value

    # ------------------------------------------------------------------------------------------------------------------

    def __init__(self, db, id, ref=False):
        self.db = db
        self._id = id
        # Variable or object referencing this object
        self.ref = ref

    def delete(self, del_ref=True):
        # 1. Delete reference if necessary
        if del_ref and self.ref:
            self.ref.delete()
            # Stop if there are more references to the current object
            if self.num_ref > 0:
                return
        elif self.num_ref > 1:  # Stop if there are more references (apart from this one) to the current object
            return
        # Delete if this point reached
        self.db.delete(self.db.tab_objs, where={'id': self.id})

    def __str__(self):
        return "{}({})".format(self.__class__.__name__, self.py)

    def exist(self, raise_error=False):
        """
        :return True if the object exists in the SQL database.
        """
        exist = self.db.select(self.db.tab_objs, column=('count(*)',), where={'id': self.id})[0][0] > 0
        if raise_error and not exist:
            raise NameError('{} Variable {} not defined'.format(self.db.sqldb, self.sql_info))
        else:
            return exist

    def transaction(self):
        """Begins a transaction in the associated database"""
        return self.db.transaction()

    def _recycle_(self, x):
        """Recicle object id and use it for a new value x"""
        if isinstance(x, object_):  # SQLebra object
            x = x.py
        self.db[self.id] = x

    # Operators ------------------------------------------------------------------

    def __eq__(self, other):
        if isinstance(other, object_):
            if id(self) == id(other):
                return True
            elif self.id == other.id:
                return True
            else:
                return self.py == other.py
        else:
            return self.py == other

    def __ne__(self, other):
        if isinstance(other, object_):
            if id(self) == id(other):
                return False
            elif self.id == other.id:
                return False
            else:
                return self.py != other.py
        else:
            return self.py != other
