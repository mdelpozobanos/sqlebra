"""
For simplicity (or so I thought) the class `Database` is divided in parts:

    + Database: Defines the core elements of the class.
    + _DictInterface: Defines the dict-list interface for the database
    + _DatabaseTable: Defines interfaces with rows from each of the tables in the database:
        cls._vars - table VARIABLES
        cls._objs - table OBJECTS
        cls._items - table ITEMS
    + _ObjectFromDatabase: Defines interfaces with objects from each of the tables in the database
        cls._vars.obj - objects from table VARIABLES
        cls._objs.obj - objects from table OBJECTS
        cls._items.obj - objects from table ITEMS
    + _DatabaseDraft: Defines a draft of rows to be added to the database.
"""

import warnings
from sqlebra import SQLebraDatabase
from ._dictinterface import _DictInterface
from sqlebra import py2sql
from sqlebra.object.object_ import object_
from ._databasetable import _DatabaseTableVariables, _DatabaseTableObjects, _DatabaseTableItems
from ._databasedraft import _DatabaseDraft
from .. import exceptions as ex


class Database(SQLebraDatabase, _DictInterface):
    """
    Base class defining a common interface with different SQL databases (e.g. SQLite, MySQL, etc). A dict-like user
    interface is defined in the inherited _DictInterface.

    See also: _DictInterface
    """

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # Database
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [Database] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [Database; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    def commit(self):
        """Commit changes to the database"""
        raise NotImplementedError

    def rollback(self):
        """Roll back changes made since the last commit"""
        raise NotImplementedError

    def close(self):
        """Close database by disconnecting from it.

        Notes
        -----
        Changes to the database under the open connection are not automatically committed before closing. In other
        words, if desired, `Database.commit` should be called before closing the database. This behaviour is
        overwritten by the behaviour of the SQL database connection. For example, if such connection is configured to
        auto-commit.
        """
        raise NotImplementedError

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
            If True, references in table VARIABLES (column 'id') will be counted.
        in_tab_objs: boolean
            If True, references in table OBJECTS (column 'id') will be counted.
        in_tab_items: boolean
            If True, references in table ITEMS (column 'child_id') will be counted
        parent_id: int or tuple
            If specified, only items with column 'id' = `parent_id` will be counted.

        Return
        ------
        Counter:
            A Counter (dict-like) object with the id as key and the counter as value
        """
        # raise NotImplementedError
        warnings.warn("Using the default, non-optimised implementation of `database._obj_ref_count`.")
        count = 0
        if in_tab_vars:
            count += self._Database__select_tab_vars(('count(*)',), where={'id': id})[0][0]
        if in_tab_objs:
            count += self._Database__select_tab_objs(('count(*)',), where={'id': id})[0][0]
        if in_tab_items:
            if parent_id is None:
                count += self._Database__select_tab_items(('count(*)',), where={'id': id})[0][0]
            else:
                if isinstance(parent_id, int):
                    parent_id = (parent_id,)
                count += self._Database__select_tab_items(('count(*)',), where={'id': id, 'parent_id': parent_id})[0][0]
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
        # raise NotImplementedError
        warnings.warn("Using the default, non-optimised implementation of `database._free_id`.")
        if not compact:
            raise NotImplementedError("Option `compact = False` not implemented.")
        self._Database__select_tab_vars(('id',))
        used_ids = set(self._Database__select_tab_vars(('id',))).union(
            self._Database__select_tab_objs(('id',))
        ).union(
            self._Database__select_tab_items(('id',))
        ).union(
            self._Database__select_tab_items(('child_id',))
        )
        if len(used_ids) == 0:  # -> empty database
            return tuple(range(num))
        free_ids = tuple(set(range(max(used_ids))) - used_ids)
        if len(free_ids <= num):
            return free_ids[:num]
        else:
            n0 = max(used_ids) + 1
            nN = n0 + num - len(free_ids)
            return free_ids + tuple(range(n0, nN))

    # [Database; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only.
    # Note that modules `_databasedraft`, `_databasetable`, `_dictinterface` and `_objectfromdatabase` are structural
    # parts of the `Database` class and can access these methods as well.

    def _Database__tab_name(self, name):
        """Return full name of a table. Usually incorporates the name of the database."""
        raise NotImplementedError

    def _Database__tab_exists(self, tab_name):
        """Return True if table `tab_name` exists in the database"""
        raise NotImplementedError

    # Structure allocation
    # --------------------

    def _Database__allocate_tab_vars(self, transaction):
        """Allocate table VARIABLES"""
        raise NotImplementedError

    def _Database__allocate_tab_objs(self, transaction):
        """Allocate table OBJECTS"""
        raise NotImplementedError

    def _Database__allocate_tab_items(self, transaction):
        """Create table ITEMS"""
        raise NotImplementedError

    # Select table
    # ------------

    def _Database__select_tab_vars(self, column=('id', 'name'), where={}):
        """Return rows from table VARIABLES.

        Parameters
        ----------
        column: list[str]
            List of columns to return. Possible values are 'id' and 'name'
        where: dict
            Dictionary specifying (column, value) pairs of the requested rows. Column names are keys in the dictionary.
            Possible keys are 'id' and 'name'. If key is '!@*', value is a full SQL comparison expression (e.g.,
            `{'!@*': 'ind < 4'}`).

        Returns
        -------
        tuple of tuples:
            A tuple containing the selected rows, with each row containing the selected columns in order.
        """
        raise NotImplementedError

    def _Database__select_tab_objs(self, column=('id', 'type', 'bool_val', 'int_val', 'real_val', 'txt_val'), where={}):
        """Return row from table OBJECTS.

        Parameters
        ----------
        column: list[str]
            List of columns to return. Possible values are 'id', 'type', 'bool_val', 'int_val', 'real_val' and/or
            'txt_val'.
        where: dict
            Dictionary specifying (column, value) pairs of the requested rows. Column names are keys in the dictionary.
            Possible keys are 'id', 'type', 'bool_val', 'int_val', 'real_val' and/or 'txt_val'. If key is '!@*', value
            is a full SQL comparison expression (e.g., `{'!@*': 'ind < 4'}`).

        Returns
        -------
        tuple[row[column]]:
            A tuple containing the selected rows, with each row containing the selected columns in order.
        """
        raise NotImplementedError

    def _Database__select_tab_items(self, column=('id', 'ind', 'key', 'child_id'), where={}):
        """Return rows from table ITEMS.

        Parameters
        ----------
        column: list[str]
            List of columns to return. Possible values are 'id', 'ind, 'key' and/or 'child_id'
        where: dict
            Dictionary specifying (column, value) pairs of the requested rows. Column names are keys in the dictionary.
            Possible keys are 'id', 'ind', 'key' and/or 'child_id'. If key is '!@*', value is a full SQL comparison
            expression (e.g., `{'!@*': 'ind < 4'}`).

        Returns
        -------
        tuple[row[column]]:
            A tuple containing the selected rows, with each row containing the selected columns in order.
        """
        raise NotImplementedError

    # Delete table
    # ------------

    def _Database__delete_tab_vars(self, where):
        """Delete rows from table VARIABLES"""
        raise NotImplementedError

    def _Database__delete_tab_objs(self, where):
        """Delete rows from table OBJECTS"""
        raise NotImplementedError

    def _Database__delete_tab_items(self, where):
        """Delete rows from table ITEMS"""
        raise NotImplementedError

    # Insert
    # ------

    def _Database__insert_tab_vars(self, values):
        """Insert rows into table VARIABLES"""
        raise NotImplementedError

    def _Database__insert_tab_objs(self, values):
        """Insert rows into table OBJECTS"""
        raise NotImplementedError

    def _Database__insert_tab_items(self, values):
        """Insert rows into table ITEMS"""
        raise NotImplementedError

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
            Possible keys are 'id' and 'name'. If key is '!@*', value is a full SQL comparison expression (e.g.,
            `{'!@*': 'ind < 4'}`).
        """
        raise NotImplementedError

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
            Possible keys are 'id', 'type', 'bool_val', 'int_val', 'real_val' and/or 'txt_val'. If key is '!@*', value
            is a full SQL comparison expression (e.g., `{'!@*': 'ind < 4'}`).
        """
        raise NotImplementedError

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
            Possible keys are 'id', 'ind, 'key' and/or 'child_id'. If key is '!@*', value is a full SQL comparison
            expression (e.g., `{'!@*': 'ind < 4'}`).
        """
        raise NotImplementedError

    # Transaction/with
    # ----------------

    def _Database__transaction_begin(self):
        """Begin transaction"""
        raise NotImplementedError

    def _Database_transaction_commit(self):
        """Commit and end transaction"""
        raise NotImplementedError

    def _Database__transaction_rollback(self):
        """Rollback and end transaction"""
        raise NotImplementedError

    # [Database] Fully defined methods/properties
    # ==================================================================================================================
    # Methods/properties fully defined here. They can be overloaded by inheriting classes

    # [Database; FD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    def __init__(self, file, name):
        """
        Parameters
        -----------
        file: str
            Full path file name of the SQL database file.
        name: str
            Name of the database.
        """
        self.file = file
        self.name = name
        self._draft = _DatabaseDraft(self)  # A draft of the database
        self._Database__transaction_level = 0  # Transaction level. 0 means no transaction, inf means manual commit
        # Tables
        self._vars = _DatabaseTableVariables(self)
        self._objs = _DatabaseTableObjects(self)
        self._items = _DatabaseTableItems(self)
        # Table names
        self._name_tab_vars = self._Database__tab_name('vars')
        self._name_tab_objs = self._Database__tab_name('objs')
        self._name_tab_items = self._Database__tab_name('items')

    @property
    def info(self):
        """File and database information string"""
        return "[file:'{}'][db:'{}']".format(self.file, self.name)

    @property
    def py(self):
        """Dictionary with all variables defined in the database"""
        return dict(tuple((name, self._vars.obj[name].py) for name in (v[0] for v in self._vars[:, 'name'])))

    @property
    def objs(self):
        """Dictionary with all variables defined in the database as SQLebra objects"""
        return dict(tuple((name, self._vars.obj[name]) for name in (v[0] for v in self._vars[:, 'name'])))

    def clear(self):
        """Drop database structure / tables"""
        with self as trans:
            trans._execute("drop table {}".format(self._name_tab_vars))
            trans._execute("drop table {}".format(self._name_tab_objs))
            trans._execute("drop table {}".format(self._name_tab_items))
        return self

    def check(self):
        """Check integrity of the database"""
        # Check that structural tables exists
        num_tabs = self._Database__tab_exists(self._Database__tab_name('vars')) + \
                   self._Database__tab_exists(self._Database__tab_name('objs')) + \
                   self._Database__tab_exists(self._Database__tab_name('items'))
        if num_tabs == 0:
            return
        elif num_tabs < 3:
            raise ex.CorruptedDatabase('Not all tables are defined in the database')
        # Extract identifiers
        vars_id = self._vars[:, 'id']
        objs_id = self._objs[:, 'id']
        items_id = self._items[:, 'id']
        items_child_id = self._items[:, 'child_id']
        # All variable ids must be in table OBJECTS
        error_msg = []
        for id in vars_id:
            if id not in objs_id:
                error_msg.append('Variable id {} not in table OBJECTS'.format(id))
        # All object ids in items must be in table OBJECTS
        for id in items_id:
            if id not in objs_id:
                error_msg.append('Object id {} from table ITEMS not in table OBJECTS'.format(id))
        # All items ids must be in table OBJECTS
        for id in items_child_id:
            if id not in objs_id:
                error_msg.append('Item id {} not in table OBJECTS'.format(id))
        # All object ids must be mentioned in tables VARIABLES or ITEMS
        for id in objs_id:
            if not (id in vars_id or id in items_id or id in items_child_id):
                error_msg.append('Object id {} not referenced in the database'.format(id))
        if len(error_msg) > 0:
            raise ex.CorruptedDatabase('\n\t' + '\n\t'.join(error_msg))

    def fix(self):
        """Deletes non-referenced objects (i.e. objects not referenced in the variables or table ITEMS)"""
        len_vars, len_objs, len_items = 0, 0, 0
        len_vars_n, len_objs_n, len_items_n = self._select(self._name_tab_vars, column=('count(*)',))[0][0], \
                                              self._select(self._name_tab_objs, column=('count(*)',))[0][0], \
                                              self._select(self._name_tab_items, column=('count(*)',))[0][0]
        while not (len_vars == len_vars_n and len_objs == len_objs_n and len_items == len_items_n):
            len_vars, len_objs, len_items = len_vars_n, len_objs_n, len_items_n
            # Delete objects
            self._execute("""
                delete from {objs}
                where id not in (select child_id from {items})
                and id not in (select id from {vars})
                """.format(objs=self._name_tab_objs, items=self._name_tab_items, vars=self._name_tab_vars))
            # Delete items
            self._execute("""
                delete from {items}
                where id not in (select child_id from {items})
                and id not in (select id from {vars})
                """.format(items=self._name_tab_items, vars=self._name_tab_vars))

    # transaction/with
    # ----------------

    def __enter__(self):
        self._Database__transaction_level += 1
        if self._Database__transaction_level == 1:
            self._Database__transaction_begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._Database__transaction_level -= 1
        if exc_type:
            self._Database__transaction_rollback()
            exc_type(exc_val, exc_tb)
        elif self._Database__transaction_level == 0:
            self._Database_transaction_commit()

    # pickle support
    # --------------
    #
    # def __getstate__(self):
    #     if self._Database__conx_ is None:
    #         return {'file': self._file, 'name': self.name}
    #     else:
    #         return {'file': self._file, 'name': self.name, '__open__': True}
    #
    # def __setstate__(self, state):
    #     if state.pop('__open__', False):
    #         self.__init__(**state)
    #         self.open()
    #     else:
    #         self.__init__(**state)

    # [Database; FD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    # Insert
    # ------

    def _draft_obj(self, value):
        """
        Draft a value's rows from tables OBJECTS and ITEMS representing the value in the database, and returns its
        object identifier.

        Parameters
        ----------
        value: `python variable`
            Any python variable

        Returns
        -------
        int:
            Unique object identifier for value. If the value is <0 then the object is new and it is in drafted mode.
            This can be consolidated calling `self._draft.commit`.

        Notes
        -----
        The drafting of multi objects (see sqlebra.object.multi) is decentralised as these classes can have very
        different drafting patterns. Thus, this aims to support the drafting of multi objects.
        """
        if isinstance(value, object_):  # -> value is an SQLebra object
            if value.db == self:  # -> value is stored in this database, just point to it
                return value._id
            else:  # -> value stored in a different database.
                value = value.py

        try:
            return py2sql.py2sql[type(value)]._draft_obj(self._draft, value)
        except IndexError:
            raise TypeError('SQLebra does not support type {}'.format(type(value)))

    def _commit_draft(self, *args, **kwargs):
        """Apply drafted changes to the database"""
        return self._draft._commit(*args, **kwargs)

    # [Database; FD] Database private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by Database only

    def _Database__row2obj(self, row_id, row_type, ref):
        """Convert a row from table OBJECTS to an SQLebra object

        Parameters
        ----------
        row_id: int
            Unique object identifier
        row_type: python class
            Object's python class
        ref: SQLebra.Variable or SQLebra.Item
            SQLebra variable or item requesting the object

        Returns
        -------
        SQLebra object:
            An SQLebra object representing the selected row.
        """
        try:
            return py2sql.py2sql_str[row_type](db=self, id=row_id, ref=ref)
        except KeyError:
            raise TypeError('Type {} not supported by SQLebra'.format(row_type))

    # Structure allocation
    # --------------------

    def _Database__allocate_structure(self):
        """Initialize database structure"""
        with self:
            if not self._Database__tab_exists(self._name_tab_vars):
                self._Database__allocate_tab_vars()
            if not self._Database__tab_exists(self._name_tab_objs):
                self._Database__allocate_tab_objs()
            if not self._Database__tab_exists(self._name_tab_items):
                self._Database__allocate_tab_items()
