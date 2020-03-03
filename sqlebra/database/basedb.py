import os
import pydash
from itertools import compress
from sqlebra.utils import argsort, rank_list
from sqlebra.object.variable import Variable
from sqlebra.object.object_ import object_
from sqlebra import exceptions as ex
from sqlebra import py2sql
from .basetransaction import BaseTransaction


class BaseDB:
    """
    Base class holding an SQL database handler, agnostic to the type of SQL.

    A databae is composed of 3 tables:
        objects: Contains the values defined in the database. Concept similar to python's "object".
        items: Contains pointers for items of nested objects.
        variables: Contains user defined named variables pointing to an object. Concept similar to python's "variable".
    """

    @property
    def py(self):
        """Dictionary with all variables defined in the database"""
        # TODO. This should return all variables defined in the database
        x = self[:][2]
        py = {}
        for x_n in x:
            py[x_n.row[1]] = x_n.py
        return py

    @property
    def file(self):
        """Full path file name of the SQL database file"""
        return self._file

    @property
    def sqldb(self):
        return "[file:'{}'][db:'{}']".format(self.file, self.name)

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
        # Database names
        self.name = name
        self.tab_vars = self._tab_name('vars')
        self.tab_objs = self._tab_name('objs')
        self.tab_items = self._tab_name('items')

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
        """
        Initialize database structure

        A database has 2 tables:
            <name>_objs: Contains objects with values.
            <name>_vars: Contains user defined variables pointing to objects from <name>_objs
        """
        # Create table variables
        self.execute(
            'create table {} ('.format(self.tab_vars) +
            'name VARCHAR(100) UNIQUE, '
            'id BIGINT'
            ')')
        # Create table objects
        self.execute(
            'create table {} ('.format(self.tab_objs) +
            'id BIGINT UNIQUE,'
            'type VARCHAR(100),'
            'bool_val TINYINT(1),'
            'int_val BIGINT,'
            'real_val DOUBLE,'
            'txt_val TEXT'
            ')')
        # Create table objects
        self.execute(
            'create table {} ('.format(self.tab_items) +
            'id BIGINT,'
            'key VARCHAR(500),'
            'ind BIGINT,'
            'child_id BIGINT'
            ')')
        return self

    def clear(self):
        """Delete database structure"""
        self.execute("drop table {}".format(self.tab_vars))
        self.execute("drop table {}".format(self.tab_objs))
        self.execute("drop table {}".format(self.tab_items))
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

    def _tab_name(self, name):
        """Builds full name of a table"""
        raise NotImplemented

    def execute(self, query, pars=False, fetch=False):
        """
        Execute an sql query.

        :param query: (str) SQL query
        :param pars: (list) Parameters required by the SQL query.
        :return: Result of the SQL query.
        """
        raise NotImplementedError

    def insert(self, table, value):
        """
        Insert values in table objects.

        :param table: (str) Name of table to insert.
        :param value: (dict) with (key, value) = (column, value). Builds the "(<column>) value (<value>)"
            clause of the SQL insert query. To insert multiple values at once, use list values of the same length for
            all keys.
        """
        # Build insert query
        into_sql = ', '.join([k for k in value.keys()])
        col_sql = ', '.join(['?']*len(value))
        val_sql = [v for v in value.values()]
        # Execute query
        self.execute("insert into {} ({}) values ({})".format(table, into_sql, col_sql), val_sql)

    @staticmethod
    def _build_where(where):
        """
        Convert a 'where' dictionary into an sql where clause
        :param where:  (dict) with (key, value) = (columns, value) in the table. Key 'rn' is a especial name used to
            select a specific row index.
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

    def update(self, table, set, where):
        """
        Update value in table objects

        :param table: (str) Name of table.
        :param set: (dict) with keys = columns in <table> and values = columns' set values. Builds
            the "set" clause of the update SQL query.
        :param where: (dict) with (key, value) = (columns, value) in <table>. Builds
            the "where" clause of the SQL update query.
        """
        # Build set
        set_sql = []
        set_values = []
        for key, value in set.items():
            if key == '*':  # Literal expression
                set_sql.append(value)
            else:
                set_sql.append('{} = ?'.format(key))
                set_values.append(value)
        set_sql = ', '.join(set_sql)
        # Build where
        where_sql, where_values, limit_sql = self._build_where(where)
        # Update
        self.execute("update {} set {} {} {}".format(table, set_sql, where_sql, limit_sql),
                     set_values + where_values)

    def select(self, table, column=[], where={}, order=[], order_list=None):
        """
        Select from table

        :param table: (str) Name of table.
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
        res = self.execute("select {} from {} {} {} {}".format(col_sql, table, where_sql, order_sql, limit_sql),
                           where_values, fetch=True)
        # Order by list
        if order_list:
            # TODO: Check this posibility
            # if len(order_list) == len(res):
            #     order_ind = argsort(order_list)
            #     return [res[i] for i in order_ind]
            # else:
            return [res[i] for i in rank_list(order_list)]
        else:
            return res

    def delete(self, table, where):
        """
        Delete from table

        :param table: (str) Name of table.
        :param where: (dict) with (key, value) = (columns, value) in <table>. Builds
            the "where" clause of the SQL delete query.
        """
        # Build where
        where_sql, where_values, limit_sql = self._build_where(where)
        # Delete
        self.execute('delete from {} {} {}'.format(table, where_sql, limit_sql), where_values)

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
        exists = 0
        for tab in (self.tab_vars, self.tab_objs, self.tab_items):
            exists += self._exists_(tab)
        return exists == 3

    def free_id(self, num=1, compact=False):
        """
        Return an available id

        :param num: (int) Number of ids to return.
        :param compact: (bool) If true, find the smallest ids available.
        :return: (int) id
        """
        if compact:
            # 1. Find free ids to the left of the minimum id
            min_id = self.execute('select min(id) from {}'.format(self.tab_objs), fetch=True)[0][0]
            if min_id is None:  # Empty table
                return tuple(range(num))
            if min_id > 0:
                free_id = tuple(range(min(min_id, num)))
            else:
                free_id = ()
            # 2. Find free ids between the minimum and maximum used
            if len(free_id) < num:
                edges = self.execute(
                    'with aux_ids as (select distinct id from {}), '.format(self.tab_objs) +
                    'unique_ids as ('
                    '   select id, (select count(*) from aux_ids b where a.id >= b.id) as rn from aux_ids a '
                    ') '
                    'select a.id, b.id '
                    'from unique_ids a '
                    'join unique_ids b '
                    'on a.rn = b.rn-1 '
                    'where b.id - a.id > 1 '
                    'limit {} '.format(num - len(free_id)),
                    fetch=True
                )
                for edge in edges:  # edge = edges[0]
                    free_id += tuple(range(edge[0]+1, min(edge[1], edge[0] + 1 + num - len(free_id))))
                    if len(free_id) == num:
                        break
            # 3. Find ids to the right of the maximum id
            if len(free_id) < num:
                max_id = self.execute('select max(id) from {}'.format(self.tab_objs), fetch=True)[0][0]
                return free_id + tuple(n for n in range(max_id + 1, max_id + num + 1 - len(free_id)))
            else:
                return free_id
        else:
            first_id = self.execute("select coalesce(max(id) + 1, 0) from {}".format(self.tab_objs), fetch=True)[0][0]
            return tuple(n for n in range(first_id, first_id+num))

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
        raise NotImplemented
        # Build where
        where_sql, where_values, limit_sql = self._build_where(where)
        rows = self.execute('select * from [db] {} {}'.format(where_sql, limit_sql), where_values)
        raise NotImplementedError

    def __row2obj__(self, row_id, row_type):
        """Support method used in __getitem__"""
        if row_type in py2sql.py2sql_single_str:
            return py2sql.py2sql_single_str[row_type](db=self, id=row_id)
        elif row_type in py2sql.py2sql_nested_str:
            return py2sql.py2sql_nested_str[row_type](db=self, id=row_id)
        else:
            raise TypeError('Type {} not supported by SQLebra'.format(row_type))

    def __getitem__(self, item):
        """
        :param item:
            (if str) Variable name: Queries table Variables. Return a single object
            (if int) Object id: Queries table Objects. Return a single object
            (if list) Multiple object id: Queries table Objects. Return list of objects
            (if tuple) Multiple object id: Queries table Objects. Return tuple of objects
            (if dict) SQL where clause: Queries table Items. Return tuple of objects
        :return:
        """

        if isinstance(item, str):  # Retrieve variable
            var = Variable(self, item)
            if not var.exist():
                raise ex.VariableError("{} Variable '{}' not found".format(self.sqldb, item))
            return var.obj
        if isinstance(item, int):
            row = self.select(self.tab_objs, column=('id', 'type'), where={'id': item})
            if len(row) == 0:
                raise ex.ObjectError("{} Object '{}' not found".format(self.sqldb, item))
            elif len(row) == 1:
                return self.__row2obj__(row[0][0], row[0][1])
            else:
                raise ex.CorruptedDatabase("{} Multiple objects '{}'".format(self.sqldb, item))
        elif isinstance(item, list):
            row = self.select(self.tab_objs, column=('id', 'type'), where={'id': item},
                              order=('id', ), order_list=item)
            return list(self.__row2obj__(row_n[0], row_n[1]) for row_n in row)
        elif isinstance(item, tuple):
            row = self.select(self.tab_objs, column=('id', 'type'), where={'id': list(item)},
                              order=('id', ), order_list=item)
            return tuple(self.__row2obj__(row_n[0], row_n[1]) for row_n in row)
        elif isinstance(item, dict):
            # Return items
            if 'order_list' in item:
                row = self.select(self.tab_items, column=('child_id', ), where=pydash.omit(item, 'order_list'),
                                  order=('id', 'ind', 'key'), order_list=item['order_list'])
            else:
                row = self.select(self.tab_items, column=('child_id', ), where=item, order=('id', 'ind', 'key'))
            return tuple(self[row_n[0]] for row_n in row)

    def __value2dict__(self, value):
        """Support method used in __setitem__"""
        if isinstance(value, object_):  # SQLebra object
            # If object exists in this database, we can just point to it
            if value.db == self:
                return {'new': False, 'id': value.id}
            else:  # Object from another database. Add as new object to this database
                value = value.py
        if type(value) in py2sql.py2sql_single:  # Single object
            return py2sql.py2sql_single[type(value)].value2row(value)
        elif type(value) in py2sql.py2sql_nested:  # Nested object
            res = py2sql.py2sql_nested[type(value)].value2row(value)
            res['nested'] = True
            return res
        else:
            raise TypeError('SQLebra does not support type {}'.format(type(value)))

    def __setitem__(self, item, value):
        """
        :param item:
            (if str) Variable name: Adds to table Variables.
            (if int) Object id: Adds to table Objects.
            (if list) Multiple object id: Adds to table Objects.
                item is expected to be a list of ids and value to be an iterable of the same length as item. All items
                    in value will be added as individual objects with the corresponding id specified in item.
                If item[n] is None, an id is automatically generated and overwritten to item[n].
                If value[n] is an object defined in this database, item[n] will be overwritten with the id of the
                    SQL-object value[n].
                *NOTE*: In this scenario, item is an input-output variables than can be used to locate the ids of the
                    inserted objects
            (if dict) SQL where clause: Adds to table Items.
        """

        # Multiple values inserted
        # ------------------------

        if isinstance(item, list):

            # Build the insert_dict
            len_value = len(value)
            insert_dict = {'new': [True] * len_value,
                           'nested': [False] * len_value,
                           'id': item,
                           'type': [None] * len_value,
                           'bool_val': [None] * len_value,
                           'int_val': [None] * len_value,
                           'real_val': [None] * len_value,
                           'txt_val': [None] * len_value,
                           }
            for n, v in enumerate(value):
                for k, i in self.__value2dict__(v).items():
                    insert_dict[k][n] = i

            # Get flags new_object and nested_object
            new_object = insert_dict.pop('new')
            nested_object = insert_dict.pop('nested')
            # Convert values to tuples
            if all(new_object):  # All inserted values are new
                for key in insert_dict.keys():
                    insert_dict[key] = tuple(compress(insert_dict[key], new_object))
            elif not any(new_object):  # No inserted value is new
                return
            else:  # Some objects already exist. Remove exist objects from the inserting dictionary
                nested_object = list(compress(nested_object, new_object))
                for key in insert_dict.keys():
                    insert_dict[key] = tuple(compress(insert_dict[key], new_object))
            # Insert
            self.insert(self.tab_objs, insert_dict)
            # Add body of values with nested objects
            for n in compress(range(len_value), nested_object):
                self[insert_dict['id'][n]].py = value[n]
            # Done
            return

        # Single value inserted
        # ---------------------

        # Build insert_dict
        insert_dict = self.__value2dict__(value)
        # If this is a new object and no id has been specified, assign a free id to it.
        if 'new' in insert_dict:  # NOT NEW! WHAT? key 'new' only added if the object is not new (see __value2dict__)
            insert_dict.pop('new')
            if isinstance(item, dict):  # If item is a dict overwrite id
                item['child_id'] = insert_dict['id']
        elif isinstance(item, int):  # If item is an integer, item is the id
            insert_dict['id'] = item
        elif isinstance(item, dict):  # If item is a dict, it may provide the id
            if 'child_id' in item:
                insert_dict['id'] = item['child_id']
            else:  # Generate new id
                insert_dict['id'] = self.free_id()[0]
                item['child_id'] = insert_dict['id']
        else:  # isinstance(item, str)  # Generate new id
            insert_dict['id'] = self.free_id()[0]
        # Check item type
        if isinstance(item, str):  # Variable name provided
            if item in self:
                var = Variable(self, item)
                # Detach variable from object
                var.obj.delete(False)
                # Update id
                var.id = insert_dict['id']
            else:  # New variable
                self.insert(self.tab_vars, {'name': item, 'id': insert_dict['id']})
        elif isinstance(item, dict):  # Item provided
            row = self.select(self.tab_items, where=item)
            if len(row) == 0:
                self.insert(self.tab_items, item)
            else:
                a = 10
                pass
        # Insert object
        if 'nested' in insert_dict:
            nested_object = insert_dict.pop('nested')
        else:
            nested_object = False
        self.insert(self.tab_objs, insert_dict)
        if nested_object:
            self[insert_dict['id']].py = value

    def __len__(self):
        return self.execute('select count(*) from {}'.format(self.tab_vars))[0][0]

    def __contains__(self, item):
        # Normalize item to dictionary
        if isinstance(item, int):
            return self.select(self.tab_objs, column=('count(*)', ), where={'id': item})[0][0] == 1
        elif isinstance(item, str):
            return self.select(self.tab_vars, column=('count(*)', ), where={'name': item})[0][0] == 1
        elif isinstance(item, dict):
            return self.select(self.tab_objs, column=('count(*)', ), where=item)[0][0] > 0

    def __eq__(self, other):
        if not isinstance(other, BaseDB):
            return False
        elif id(self) == id(other):
            return True
        elif self.file == other.file and self.name == other.name:
            return True
        else:
            return False
