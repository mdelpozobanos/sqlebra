
class Object:
    """Object defined in the SQL database"""

    @property
    def id(self): return self.row[0]
    @property
    def name(self): return self.row[1]
    @property
    def py_class(self): return self.row[2]
    @property
    def key(self): return self.row[3]
    @property
    def ind(self): return self.row[4]
    @property
    def bool_val(self): return self.row[5]
    @property
    def int_val(self): return self.row[6]
    @property
    def real_val(self): return self.row[7]
    @property
    def txt_val(self): return self.row[8]
    @property
    def child_id(self): return self.row[9]
    @property
    def root(self): return self.row[10]
    @property
    def user_defined(self): return self.row[11]

    @property
    def py(self):
        raise NotImplemented

    def __init__(self, db, row):
        self.db = db
        if isinstance(row, dict):
            self.row = (row['id'], row['name'], row['dtype'], row['key'], row['ind'], row['int_val'],
                        row['real_val'], row['txt_val'], row['child_id'])
        elif isinstance(row, list):
            self.row = tuple(row)
        elif isinstance(row, tuple):
            self.row = row

    def delete(self):
        raise NotImplemented

    def __str__(self):
        return "{}({})".format(self.__class__.__name__, self.py)

    def exist(self, raise_error=False):
        """
        :return True if the object exists in the SQL database.
        """
        exist = self.db.select(column=('count(*)',), where={'id': self.id})[0][0] > 0
        if raise_error and not exist:
            raise NameError('in database {}: variable {} not defined'.format(self.db.name, self.sql_info))
        else:
            return exist

    def transaction(self):
        return self.db.transaction()
