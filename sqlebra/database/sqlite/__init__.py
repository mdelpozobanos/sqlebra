import os
import sqlite3
from sqlebra.database.database import Database


class SQLiteDatabase(Database):
    """Class holding an SQLite database handler"""

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # Database
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [Database] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [Database; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    def __init__(self, file, name, conx, c):  # Overwrites inherited __init__
        """
        Parameters
        ----------
        conx: SQLite3 connection
        c: SQLite3 connection cursor
        """
        super().__init__(file, name)
        self._SQLiteDatabase__conx = conx
        self._SQLiteDatabase__c = c
        # Allocate structure
        self._Database__allocate_structure()

    def commit(self):
        self._SQLiteDatabase__conx.commit()
        return self

    def rollback(self):
        self._SQLiteDatabase__conx.rollback()
        return self

    def close(self):
        """Close database by disconnecting from it.

        Notes
        -----
        Changes to the database under the open connection are not automatically committed before closing. In other
        words, if desired, `Database.commit` should be called before closing the database. This behaviour is
        overwritten by the behaviour of the SQL database connection. For example, if such connection is configured to
        auto-commit.
        """
        if not self._SQLiteDatabase__conx:
            return
        self._SQLiteDatabase__c.close()
        self._SQLiteDatabase__conx.close()
        self._SQLiteDatabase__conx = None
        self._SQLiteDatabase__c = None
        return self

    # [Database; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    def _obj_ref_count(self, id, in_tab_vars=True, in_tab_objs=False, in_tab_items=True, parent_id=None):
        """Count the number of references an object has in the database.

        Parameters
        ----------
        id: int or tuple
            Unique identifier(s) of the object(s).
        in_tab_vars: boolean
            If and only if True, references in table VARIABLES will be counted.
        in_tab_objs: boolean
            If and only if True, references in table OBJECTS will be counted.
        in_tab_items: boolean
            If and only if True, references in table ITEMS will be counted
        parent_id: int or tuple
            If specified, only items with column 'id' = `parent_id` will be counted.

        Return
        ------
        Counter:
            A Counter (dict-like) object with the id as key and the counter as value
        """
        count = 0
        if isinstance(id, int):
            id = (id,)
        if in_tab_vars:
            count += self._vars[{'id': id}, 'count(*)'][0][0]
        if in_tab_objs:
            count += self._objs[{'id': id}, 'count(*)'][0][0]
        if in_tab_items:
            if parent_id is None:
                count += self._items[{'child_id': id}, 'count(*)'][0][0]
            else:
                if isinstance(parent_id, int):
                    parent_id = (parent_id,)
                count += self._items[{'id': parent_id, 'child_id': id}, 'count(*)'][0][0]
        return count

    def _free_id(self, num=1, compact=True):
        """Generate available unique identifiers.

        Parameters
        ----------
        num: int
            Number of identifiers to return.
        compact: bool
            If true, find the smallest identifiers available.

        Return
        ------
        int:
            Unique identifier
        """
        if not compact:
            raise NotImplementedError("Option `compact = False` not implemented.")
        used_ids = self._SQLiteDatabase__execute(f"""
                select distinct id 
                from (
                    select id from {self._name_tab_objs}
                    UNION
                    select id from {self._name_tab_items}
                    UNION
                    select child_id as id from {self._name_tab_items}
                )
            """, fetch=True)
        if len(used_ids) == 0:  # -> empty database
            return tuple(range(num))
        # Find free ids in the used range
        used_ids = set(r[0] for r in used_ids)
        free_ids = tuple(set(range(max(used_ids))) - used_ids)
        if len(free_ids) >= num:
            return free_ids[:num]
        else:  # -> add extra ids
            n0 = max(used_ids) + 1
            nN = n0 + num - len(free_ids)
            return free_ids + tuple(range(n0, nN))

    # [Database; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only
    # Note that modules `_databasedraft`, `_databasetable`, `_dictinterface` and `_objectfromdatabase` are structural
    # parts of the `Database` class and can access these methods as well.

    def _Database__tab_name(self, name):
        """Return full name of a table. Usually incorporates the name of the database."""
        return f'{self.name}_{name}'

    def _Database__tab_exists(self, tab_name):
        """Return True if table `tab_name` exists in the database"""
        return self._SQLiteDatabase__execute(
            f"select count(*) from sqlite_master where type='table' and name='{tab_name}'",
            fetch=True)[0][0] == 1

    # Structure allocation
    # --------------------

    def _Database__allocate_tab_vars(self):
        """Allocate table VARIABLES"""
        self._SQLiteDatabase__execute(f"""
            create table {self._name_tab_vars} (
                name varchar, 
                id bigint,
                PRIMARY KEY (name)
                )
            """)

    def _Database__allocate_tab_objs(self):
        """Allocate table OBJECTS"""
        self._SQLiteDatabase__execute(f"""
            create table {self._name_tab_objs} (
                id bigint, 
                type varchar, 
                bool_val smallint, 
                int_val bigint,
                real_val real,
                txt_val varchar,
                PRIMARY KEY (id)
                )
            """)

    def _Database__allocate_tab_items(self):
        """Create table ITEMS"""
        self._SQLiteDatabase__execute(f"""
            create table {self._name_tab_items} (
                id bigint, 
                ind bigint, 
                key varchar, 
                child_id bigint,
                PRIMARY KEY (id, ind, key)
                )
            """)

    # Table access
    # ------------

    def _Database__select_tab_vars(self, column=('id', 'name'), where={}):
        """Return rows from table VARIABLES.

        Parameters
        ----------
        column: list[str]
            List of columns to return. Possible values are 'id' and 'name'
        where: dict
            Dictionary specifying (column, value) pairs of the requested rows. Column names are keys in the dictionary.
            Possible keys are 'id' and 'name'.

        Returns
        -------
        tuple[row[column]]:
            A tuple containing the selected rows, with each row containing the selected columns in order.
        """
        return self._SQLiteDatabase__select_tab(self._name_tab_vars, column, where)

    def _Database__select_tab_objs(self, column=('id', 'type', 'bool_val', 'int_val', 'real_val', 'txt_val'), where={}):
        """Return row from table OBJECTS.

        Parameters
        ----------
        column: list[str]
            List of columns to return. Possible values are 'id', 'type', 'bool_val', 'int_val', 'real_val' and/or
            'txt_val'.
        where: dict
            Dictionary specifying (column, value) pairs of the requested rows. Column names are keys in the dictionary.
            Possible keys are 'id', 'type', 'bool_val', 'int_val', 'real_val' and/or 'txt_val'.

        Returns
        -------
        tuple[row[column]]:
            A tuple containing the selected rows, with each row containing the selected columns in order.
        """
        return self._SQLiteDatabase__select_tab(self._name_tab_objs, column, where)

    def _Database__select_tab_items(self, column=('id', 'ind', 'key', 'child_id'), where={}):
        """Return rows from table ITEMS.

        Parameters
        ----------
        column: list[str]
            List of columns to return. Possible values are 'id', 'ind, 'key' and/or 'child_id'
        where: dict
            Dictionary specifying (column, value) pairs of the requested rows. Column names are keys in the dictionary.
            Possible keys are 'id', 'ind', 'key' and/or 'child_id'.
        """
        return self._SQLiteDatabase__select_tab(self._name_tab_items, column, where)

    # Delete
    # ------

    def _Database__delete_tab_vars(self, where):
        """Delete rows from table VARIABLES"""
        self._SQLiteDatabase__delete_tab(self._name_tab_vars, where)

    def _Database__delete_tab_objs(self, where):
        """Delete rows from table OBJECTS"""
        self._SQLiteDatabase__delete_tab(self._name_tab_objs, where)

    def _Database__delete_tab_items(self, where):
        """Delete rows from table ITEMS"""
        self._SQLiteDatabase__delete_tab(self._name_tab_items, where)

    # Insert
    # ------

    def _Database__insert_tab_vars(self, values):
        """Insert rows into table VARIABLES"""
        self._SQLiteDatabase__insert_tab(self._name_tab_vars, values)

    def _Database__insert_tab_objs(self, values):
        """Insert rows into table OBJECTS"""
        self._SQLiteDatabase__insert_tab(self._name_tab_objs, values)

    def _Database__insert_tab_items(self, values):
        """Insert rows into table ITEMS"""
        self._SQLiteDatabase__insert_tab(self._name_tab_items, values)

    # Update
    # ------

    def _Database__update_tab_vars(self, set, where):
        """Update table VARIABLES

        Parameters
        ----------
        set: dict
            Dictionary specifying (column, value) pairs of the updated columns. Column names are keys in the dictionary.
            Possible keys are 'id' and 'name'. If value is a string starting with '!@*', value[:3] is interpreted as a
            full SQL expression (e.g., `{'name': "!@* name || '_extra'"}`).
        where: dict
            Dictionary specifying (column, value) pairs of the requested rows. Column names are keys in the dictionary.
            Possible keys are 'id' and 'name'.
        """
        return self._SQLiteDatabase__update_tab(self._name_tab_vars, set, where)

    def _Database__update_tab_objs(self, set, where):
        """Update table OBJECTS

        Parameters
        ----------
        set: dict
            Dictionary specifying (column, value) pairs of the updated columns. Column names are keys in the dictionary.
            Possible keys are 'id', 'type', 'bool_val', 'int_val', 'real_val' and/or 'txt_val'. If value is a string
            starting with '!@*', value[:3] is interpreted as a full SQL expression (e.g.,
            `{'int_val': '!@* int_val + 4'}`).
        where: dict
            Dictionary specifying (column, value) pairs of the requested rows. Column names are keys in the dictionary.
            Possible keys are 'id', 'type', 'bool_val', 'int_val', 'real_val' and/or 'txt_val'.
        """
        return self._SQLiteDatabase__update_tab(self._name_tab_objs, set, where)

    def _Database__update_tab_items(self, set, where):
        """Update table ITEMS

        Parameters
        ----------
        set: dict
            Dictionary specifying (column, value) pairs of the updated columns. Column names are keys in the dictionary.
            Possible keys are 'id', 'ind, 'key' and/or 'child_id'. If value is a string starting with '!@*', value[:3]
            is interpreted as a full SQL expression (e.g., `{'ind': '!@* ind + 4'}`).
        where: dict
            Dictionary specifying (column, value) pairs of the requested rows. Column names are keys in the dictionary.
            Possible keys are 'id', 'ind, 'key' and/or 'child_id'.
        """
        return self._SQLiteDatabase__update_tab(self._name_tab_items, set, where)

    # Transaction/with
    # ----------------

    def _Database__transaction_begin(self):
        """Begin transaction"""
        # Any previous changes must be committed before starting a transaction
        # self._SQLiteDatabase__execute('commit')
        self._SQLiteDatabase__execute('begin')

    def _Database_transaction_commit(self):
        """Commit and end transaction"""
        self.commit()

    def _Database__transaction_rollback(self):
        """Rollback and end transaction"""
        self.rollback()

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # SQLite
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [SQLite] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [SQLite; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    @property
    def autocommit(self):
        return self._SQLiteDatabase__conx.isolation_level is None

    # [SQLite; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [SQLite; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # [SQLite] FD
    # ==================================================================================================================
    # Methods/properties fully defined here. They can be overloaded by inheriting classes

    # [SQLite; FD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pass

    # [SQLite; FD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [SQLite; FD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    def _SQLiteDatabase__execute(self, query, pars=False, fetch=False):
        """
        Execute an sql query.

        Parameters
        ----------
        query: str
            SQL query
        pars: list
            Parameters required by the SQL query.
        fetch: bool
            If True, return result of execute

        Returns
        -------
        tuple
            Result of the SQL query.
        """
        try:
            if pars:
                if isinstance(pars[0], tuple):  # Unzip parameters
                    c = self._SQLiteDatabase__c.executemany(query, [p for p in zip(*pars)])
                else:
                    c = self._SQLiteDatabase__c.execute(query, pars)
                if fetch:
                    return tuple(c.fetchall())
            elif fetch:
                return tuple(self._SQLiteDatabase__c.execute(query).fetchall())
            else:
                self._SQLiteDatabase__c.execute(query)
        except AttributeError as exc:
            if self._SQLiteDatabase__c is None:
                raise ValueError('I/O operation on closed database')
            else:
                raise exc
        except sqlite3.Error as exc:
            raise ValueError(f'When running SQLite3 expression \n{query}\n') from exc

    def _SQLiteDatabase__select_tab(self, table_name, column, where):
        """Return selected rows and columns from a table

        Parameters
        ----------
        table_name: str
            Name of the table
        column: tuple
            Selected columns to return in order.
        where: dict[column: value]
            Dictionary with the filtering pattern for row selection
        """
        return self._SQLiteDatabase__execute(f"""
                select {', '.join(column)}
                from {table_name}
                {self._SQLiteDatabase__where_dict2sql(where)}
            """, fetch=True)

    def _SQLiteDatabase__insert_tab(self, table_name, values):
        """Insert rows into table

        Parameters
        ----------
        table_name: str
            Table name
        values: dict[column: value]
            Values inserted in the table
        """
        try:
            values_sql = ', '.join(
                tuple(str(v) for v in zip(*(values[k] for k in values.keys())))
            ).replace('None', 'NULL').replace('NULLType', 'NoneType')
        except TypeError:  # -> try assuming one value per column
            values_sql = str(
                tuple(values[k] for k in values.keys())
            ).replace('None', 'NULL').replace('NULLType', 'NoneType')
        self._SQLiteDatabase__execute(f"""
                insert into {table_name} 
                    {f"({', '.join(values.keys())})"} 
                    values 
                    {values_sql}
            """)

    def _SQLiteDatabase__update_tab(self, table_name, set_dict, where):
        """
        Parameters
        ----------
        table_name: str
            Name of table
        set_dict: dict[column: value]
            Dictionary specifying (column, value) pairs of the updated columns. Column names are keys in the dictionary.
        where: dict[column: value]
            Dictionary specifying (column, value) pairs of the requested rows. Column names are keys in the dictionary.
        """
        # Build set SQL clause
        set_sql = ''
        for column, value in set_dict.items():
            if isinstance(value, str) and value.startswith('!@*'):
                value = value[3:]
            set_sql += f"{column} = {value}, "
        # Finish building query and run
        self._SQLiteDatabase__execute(f"""
                update {table_name}
                set {set_sql[:-2]}
                {self._SQLiteDatabase__where_dict2sql(where)}
            """)

    def _SQLiteDatabase__delete_tab(self, table_name, where):
        """Delete rows from table

        Parameters
        ----------
        table_name: str
            Name of table
        where: dict[column: value]
            Dictionary with the filtering pattern for row selection
        """
        self._SQLiteDatabase__execute(f"delete from {table_name} {self._SQLiteDatabase__where_dict2sql(where)}")

    def _SQLiteDatabase__where_dict2sql(self, where_dict):
        """Convert dictionary specifying selected rows into an SQL 'where' cause

        Parameters
        ----------
        where_dict: dict
            Dict with (key, value) pairs corresponding to columns and values. If value is list or tuple, operator `in`
            is used. If key is '!@*', value is a string fully specifying the comparisson (e.g., 'ind < 4').

        Returns
        -------
        str:
            SQL 'where' clause
        """
        if len(where_dict) == 0:  # empty dictionary
            return ''
        # NOTE: Using a cumulative str is faster than appending to a list and joining the end result.
        where_sql = 'where ('
        for col, val in where_dict.items():
            if col == '!@*':  # -> special case
                where_sql += f'{val}) and ('
            else:
                if isinstance(val, (list, tuple)):
                    if len(val) > 1:
                        where_sql += f"{col} in {tuple(val)}) and ("
                    else:
                        where_sql += \
                            f"""{col} == {f"'{val[0]}'" if isinstance(val[0], str) else val[0]}) and ("""
                else:
                    where_sql += f"""{col} == {f"'{val}'" if isinstance(val, str) else val}) and ("""
        return where_sql[:-6]
    #
    # # pickle
    # # ---------------------------------------------------------------
    #
    # def __getstate__(self):
    #     return {**{'connect_args': self.connect_args}, **super(SQLiteDatabase, self).__getstate__()}


def open_sqlitedatabase(file, name='sqlebra', mode='+', autocommit=False, file_options=None, **kwargs):
    """Open database by connecting to it and allocating its structure/tables if necessary. The connection
    will remain open until the database is explicitly closed.

    Parameters
    -----------
    file: str
        Full path file name of the SQLite database file.
    name: str
        Name of the database within `file`
    mode: str
        + 'r' to read an existing file. Raises an error if the file does not exist.
        + 'x' to create a new file. Raises an error if the file exists.
        + 'w' to create a new file, and overwrite it if the file already exists.
        + '+' to open an existing file or create one if it does not exit.
    autocommit: bool, default True
        If True, creates an autocommit module
    **kwargs:
        Additional arguments passed to sqlite3.connect

    Notes
    -----
    This behaviour is overwritten by the behaviour of the SQL database connection. For example, if such connection
    is configured to be non-persistent.

    See also
    --------
    `Database.close`
    """
    if mode == 'r':
        if not os.path.exists(file):
            raise FileNotFoundError(f'File {file} not found.')
    elif mode == 'w':
        if os.path.exists(file):
            # Delete current file
            os.remove(file)
    elif mode == 'x':
        if os.path.exists(file):
            raise FileExistsError(f"File '{file}' already exists.")
    elif mode == '+':
        pass
    else:
        raise ValueError(f"Mode '{mode}' is not supported.")
    if autocommit:
        kwargs['isolation_level'] = None
    else:
        kwargs['isolation_level'] = 'DEFERRED'
    if file_options:
        file = f'file:{file}?{file_options}'
    conx = sqlite3.connect(file, **kwargs)
    c = conx.cursor()
    return SQLiteDatabase(file=file, name=name, conx=conx, c=c)
