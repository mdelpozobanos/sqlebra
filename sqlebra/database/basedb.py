import os
from sqlebra import utils
from sqlebra import exceptions as ex
from sqlebra import py2sql
from .basetransaction import BaseTransaction


class BaseDB:
    """
    Base class holding an SQL database handler
    """

    @property
    def py(self):
        x = self[{'user_defined': True, 'root': True}][2]
        py = {}
        for x_n in x:
            py[x_n.row[1]] = x_n.py
        return py

    @property
    def file(self):
        """Full path file name of the SQL database file"""
        return self._file

    def __init__(self, file, name='sqlebra', mode='+'):
        """
        :param file: Full path file name of the SQL database file
        :param mode:
            'r' to read an existing file. Raises an error if the file does not exist.
            'x' to create a new file. Raises an error if the file exists.
            'w' to create a new file, and overwrite it if the file already exists.
            '+' to open an existing file or create one if it does not exit.
        """
        if mode == 'r':
            if not os.path.exists(file):
                raise FileNotFoundError('File {} not found.'.format(file))
        elif mode == 'w':
            if os.path.exists(file):
                # Delete current file
                os.remove(file)
        elif mode == 'x':
            if os.path.exists(file):
                raise FileExistsError("File '{}' already exists.".format(file))
        elif mode == '+':
            pass
        else:
            raise ValueError("Mode '{}' is not supported.".format(mode))
        self._file = file
        # Database name
        self.name = name
        # Internal variables
        self._conx = None  # Database connection
        self._c = None  # Database connection cursor
        self._transaction_level = 0  # Transaction level. 0 means no transaction

    def open(self):
        self.connect()
        if not self.exists():
            self.init()
            self.commit()
        return self

    def init(self):
        """Initialize database table"""
        self.execute(
            'create table {} ('.format(self.name) +
            'id INTEGER,'
            'name TEXT,'
            'class TEXT,'
            'key TEXT,'
            'ind INTEGER,'
            'bool_val TINYINT(1),'
            'int_val INTEGER,'
            'real_val REAL,'
            'txt_val TEXT,'
            'child_id INTEGER,'
            'root TINYINT(1),'
            'user_defined TINYINT(1)'
            ')')
        return self

    def clear(self):
        """Delete database table"""
        self.execute("drop table {}".format(self.name))
        return self

    # Python-SQL communication channel
    # ---------------------------------------------------------------

    def connect(self):
        """Connect to database"""
        raise NotImplemented

    def disconnect(self):
        """Disconnect from database"""
        raise NotImplemented

    # Simplified SQL interface
    # ---------------------------------------------------------

    def execute(self, query, pars=False):
        """
        Execute an sql query.

        :param query: (str) SQL query
        :param pars: (list) Parameters required by the SQL query.
        :return: Result of the SQL query.
        """
        raise NotImplementedError

    def insert(self, value):
        """
        Insert values in a table.

        :param value: (dict) with (key, value) = (column, value). Builds the "(<column>) value (<value>)"
            clause of the SQL insert query.
        """
        # Build insert query
        into_sql = ''
        col_sql = ''
        val_sql = []
        for key, val in value.items():
            into_sql += ', {}'.format(key)
            col_sql += ', ?'
            val_sql.append(val)
        # Execute query
        self.execute("insert into {} ({}) values ({})".format(self.name, into_sql[2:], col_sql[2:]), val_sql)

    @staticmethod
    def _build_where(where):
        """
        Convert a 'where' dictionary into an sql clause
        :param where:  (dict) with (key, value) = (columns, value) in the table.
        :return: where_sql, where_values, limit_sql
        """
        # Build limit
        if 'rn' in where:
            limit_sql = 'limit 1 offset {}'.format(where['rn'])
        else:
            limit_sql = ''
        # Build where
        where_sql = ''
        where_values = []
        for key, value in where.items():
            if isinstance(value, list):
                if len(value) == 1:
                    where_sql += ' and {} = {}'.format(key, value[0])
                else:
                    where_sql += ' and {} in {}'.format(key, tuple(value))
            elif key == '*':  # Literal where clause
                where_sql += ' and {}'.format(value)
            elif key == 'rn':  # Ignore
                pass
            else:
                if value is None:
                    where_sql += ' and {} is Null'.format(key)
                else:
                    where_sql += ' and {} = ?'.format(key)
                    where_values.append(value)
        if len(where_sql) > 0:
            where_sql = 'where ' + where_sql[5:]
        # Done
        return where_sql, where_values, limit_sql

    def update(self, set, where):
        """
        Update value in a table

        :param table: (str) 'objects' or 'values'
        :param set: (dict) with keys = columns in <table> and values = columns' set values. Builds
            the "set" clause of the update SQL query.
        :param where: (dict) with (key, value) = (columns, value) in <table>. Builds
            the "where" clause of the SQL update query.
        """
        # Build set
        set_sql = ''
        set_values = []
        for key, value in set.items():
            if key == '*':  # Literal expression
                set_sql = value
            else:
                set_sql += ', {} = ?'.format(key)
                set_values.append(value)
        if len(set_values) > 0:
            set_sql = set_sql[2:]
        # Build where
        where_sql, where_values, limit_sql = self._build_where(where)
        # Update
        self.execute("update {} set {} {} {}".format(self.name, set_sql, where_sql, limit_sql),
                     set_values + where_values)

    def select(self,  column=[], where={}, order=[]):
        """
        Select from table

        :param column: (list of str) Names of columns to select from <table>.
        :param where: (dict) with (key, value) = (columns, value) in <table>. Builds
            the "where" clause of the SQL select query.
        :param order: (list of str) Names of columns to order the result.
        """
        # Build select
        if len(column) == 0:
            col_sql = '*'
        else:
            col_sql = ''
            for c in column:
                if not isinstance(c, str):
                    raise TypeError("Selected arguments must be of type 'str'. Found {} instead.".format(type(c)))
                col_sql += ', {}'.format(c)
            col_sql = col_sql[2:]
        # Build where
        where_sql, where_values, limit_sql = self._build_where(where)
        # Build order
        if len(order) == 0:
            order_sql = ''
        else:
            order_sql = 'order by'
            for c in order:
                if not isinstance(c, str):
                    raise TypeError("Selected arguments must be of type 'str'. Found {} instead.".format(type(c)))
                order_sql += ' {},'.format(c)
            order_sql = order_sql[:-1]
        # Select
        return self.execute("select {} from {} {} {} {}".format(col_sql, self.name, where_sql, order_sql, limit_sql),
                            where_values)

    def delete(self, where):
        """
        Delete from table

        :param where: (dict) with (key, value) = (columns, value) in <table>. Builds
            the "where" clause of the SQL delete query.
        """
        # Build where
        where_sql, where_values, limit_sql = self._build_where(where)
        # Delete
        self.execute('delete from {} {} {}'.format(self.name, where_sql, limit_sql), where_values)

    def transaction(self, *args, **kwargs):
        """Return a database transaction class"""
        # Build database transaction class
        class DBTransaction(BaseTransaction, self.__class__):
            pass
        return DBTransaction(self, *args, **kwargs)

    def commit(self):
        raise NotImplemented

    def rollback(self):
        raise NotImplemented

    def exists(self):
        raise NotImplemented

    def free_id(self, consecutive=True):
        """
        Return an available id

        :param consecutive: (bool) If true, find the smallest id available.
        :return: (int) id
        """
        if consecutive:
            id = self.execute(
                'with roots as (select id from {} where root = 1) '.format(self.name) +
                'select a.id + 1 ' 
                'from roots a '
                'left outer join roots b '
                'on a.id+1 = b.id '
                'where b.id is NULL '
                'limit 1 '
            )
            if len(id) == 0:
                return 0
            else:
                return id[0][0]
        else:
            return self.execute("select coalesce(max(id) + 1, 0) from {} where root = 1".format(self.name))[0][0]

    def free_name(self, prefix=''):
        """
        Return an available name

        :param prefix: (str) A name prefix
        :return: (str) A free randomly generated object name, with the specified prefix.
        """
        name_len = 3
        while True:
            name = prefix + utils.random_str(name_len)
            name_count = self.execute(
                'select count(*) '
                'from {} '.format(self.name) +
                'where name = ? '
                'limit 1 '
                , (name,))
            if name_count[0][0] == 0:
                break
            else:
                name_len += 1
        return name

    # with
    # ---------------------------------------------------------------

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        if exc_type:
            exc_type(exc_val, exc_tb)

    # SQLfile as dictionary
    # ---------------------------------------------------------------

    def __getobject__(self, where):
        # Build where
        where_sql, where_values, limit_sql = self._build_where(where)
        rows = self.execute('select * from [db] {} {}'.format(where_sql, limit_sql), where_values)
        raise NotImplementedError

    def __norm_item__(self, item):
        if isinstance(item, dict):
            return item
        elif isinstance(item, int):
            return {'id': item}
        elif isinstance(item, str):
            return {'name': item}
        else:
            raise TypeError('Item must be of type str, int or dict. Found {} instead'.format(type(item)))

    def __getitem__(self, item):
        """
        :param item:
            (if str) Object name. Return a single value
            (if int) Object id. Return a single value
            (if dict) SQL where clause
        :return:
        """
        item_type = type(item)
        item = self.__norm_item__(item)
        if item_type == dict:
            # Keys and indices returned
            keys = []
            inds = []
        else:  # Single variable selected. Return root row. No keys and indices needed
            item['root'] = True
        row = self.select(where=item, order=('id', 'ind', 'key'))
        if len(row) == 0:
            raise ex.VariableError('in database {}: variable {} not found'.format(self.name, item))
        values = []
        for row_n in row:
            if item_type == dict:
                keys.append(row_n[3])
                inds.append(row_n[4])
            if row_n[2] in py2sql.py2sql_single_str:
                values.append(py2sql.py2sql_single_str[row_n[2]](db=self, row=row_n))
            elif row_n[2] in py2sql.py2sql_nested_str:
                if row_n[9]:
                    values.append(self[row_n[9]])
                else:
                    values.append(py2sql.py2sql_nested_str[row_n[2]](db=self, row=row_n))
            else:
                raise TypeError('Type {}, non-native to SQLebra, is not supported'.format(row[2]))
        if item_type == dict:
            return keys, inds, values
        else:
            return values[0]

    def __setitem__(self, item, value):
        item_type = type(item)
        item = self.__norm_item__(item)
        try:
            obj = self[item]
        except ex.VariableError:
            pass  # New variable
        else:
            obj = obj[2][0]
            if obj.pyclass == type(value):  # Update value and return
                obj.py = value
                return
            else:  # Delete and continue to inser
                obj.delete()
        # When item is str, this is a user defined variable
        if item_type is str:
            item['id'] = self.free_id()
            item['user_defined'] = True
        if 'root' not in item:
            item['root'] = True
        # Autodetect type
        if type(value) in py2sql.py2sql_single:
            item = {**item, **py2sql.py2sql_single[type(value)].value2row(value)}
            # Insert variable
            self.insert(item)
        elif type(value) in py2sql.py2sql_nested:
            if item['root']:
                # Convert value to row, insert variable and set value
                item = {**item, **py2sql.py2sql_nested[type(value)].value2row(value)}
                self.insert(item)
                self[item][2][0].py = value
            else:
                # Insert new variable as children
                item['child_id'] = self.free_id()
                # Convert value to row and insert variable
                item = {**item, **py2sql.py2sql_nested[type(value)].value2row(value)}
                self.insert(item)
                # Insert new nested value in the database
                nested_item = {
                    'id': item['child_id'],
                    'name': item['name'],
                    'root': True,
                    'user_defined': False
                }
                self[nested_item] = value

        elif type(value) in py2sql.single or type(value) in py2sql.nested:
            if value.dbfile != self.db:
                value = type(value)(db_file=self.db, name=value.name).set(value.x)
        else:
            raise TypeError('Type {} not supported by SQLebra'.format(type(value)))

    def __len__(self):
        return self.execute('select count(*) from {} where user_defined = 1 and root = 1'.format(self.name))[0][0]

    def __contains__(self, item):
        item = self.__norm_item__(item)
        item['root'] = True
        item['user_defined'] = True
        return len(self.select(where=item)) > 0
