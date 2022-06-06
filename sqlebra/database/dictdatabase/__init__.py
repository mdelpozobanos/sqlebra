import os
from sqlebra.database.database import Database
from sqlebra import exceptions as ex
Z = 'z'*1000

# TODO: Add support for transactions. The current implementation of transactions is not optimal as it makes certain
#  operations


class DictDatabase(Database):
    """Dict-like SQL database. This is mostly useful for debugging purposes."""

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # Database
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [Database] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [Database; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    def __init__(self, file, name, dump_args):  # Overwrites inherited __init__
        super().__init__(file, name)
        self.dump_args = dump_args
        self._DictDatabase__database = {}  # Keep database in a dictionary
        # Allocate structure
        self._Database__allocate_structure()

    def commit(self):
        raise NotImplementedError('Not implemented for DictDatabase yet')

    def rollback(self):
        raise NotImplementedError('Not implemented for DictDatabase yet')

    # [Database; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    def _obj_ref_count(self, id, parent_id=None, in_tab_vars=True, in_tab_objs=False, in_tab_items=True):
        """Count the number of references an object has in the database.

        Parameters
        ----------
        id: int or tuple
            Unique identifier(s) of the object(s).
        parent_id: int or tuple
            If specified, only references by the specified parent_id(s) will be counted.
        in_tab_vars: boolean
            If and only if True, references in table VARIABLES will be counted.
        in_tab_objs: boolean
            If and only if True, references in table OBJECTS will be counted.
        in_tab_items: boolean
            If and only if True, references in table ITEMS will be counted

        Return
        ------
        Counter:
            A Counter (dict-like) object with the id as key and the counter as value
        """
        count = 0
        if isinstance(id, int):
            id = (id,)
        if in_tab_vars:
            for id_i in self._DictDatabase__database[self._name_tab_vars]['id']:
                if id_i in id:
                    count += 1
        if in_tab_objs:
            for id_i in self._DictDatabase__database[self._name_tab_objs]['id']:
                if id_i in id:
                    count += 1
        if in_tab_items:
            if parent_id is None:
                for id_i in self._DictDatabase__database[self._name_tab_items]['child_id']:
                    if id_i in id:
                        count += 1
            else:
                if isinstance(parent_id, int):
                    parent_id = (parent_id,)
                for id_i, parent_id_i in zip(self._DictDatabase__database[self._name_tab_items]['id'],
                                             self._DictDatabase__database[self._name_tab_items]['parent_id']):
                    if id_i in id and parent_id_i in parent_id:
                        count += 1
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
        used_ids = set(self._DictDatabase__database[self._name_tab_vars]['id']).union(
            set(self._DictDatabase__database[self._name_tab_objs]['id'])
        ).union(
            set(self._DictDatabase__database[self._name_tab_items]['id'])
        ).union(
            set(self._DictDatabase__database[self._name_tab_items]['child_id'])
        )
        if len(used_ids) == 0:  # -> empty database
            return tuple(range(num))
        free_ids = tuple(set(range(max(used_ids))) - used_ids)
        if len(free_ids) >= num:
            return free_ids[:num]
        else:
            n0 = max(used_ids) + 1
            nN = n0 + num - len(free_ids)
            return free_ids + tuple(range(n0, nN))

    # [Database; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only
    # Note that modules `_databasedraft`, `_databasetable`, `_dictinterface` and `_objectfromdatabase` are structural
    # parts of the `Database` class and can access these methods as well.

    def _Database__connect(self):
        """Connect to database. Must allocate properties `_conx_` and `_c_`."""
        self._conx_ = True
        self._c_ = True

    def _Database__disconnect(self):
        """Disconnect from database. Must disconnect properties `_conx_` and `_c_` and set them to None."""
        self._conx_ = None
        self._c_ = None

    def _Database__tab_name(self, name):
        """Return full name of a table. Usually incorporates the name of the database."""
        return '{}_{}'.format(self.name, name)

    def _Database__tab_exists(self, tab_name):
        """Return True if table `tab_name` exists in the database"""
        return tab_name in self._DictDatabase__database

    # Structure allocation
    # --------------------

    def _Database__allocate_tab_vars(self):
        """Allocate table VARIABLES"""
        self._DictDatabase__database[self._name_tab_vars] = {'name': [], 'id': []}

    def _Database__allocate_tab_objs(self):
        """Allocate table OBJECTS"""
        self._DictDatabase__database[self._name_tab_objs] = {
            'id': [], 'type': [], 'bool_val': [], 'int_val': [], 'real_val': [], 'txt_val': []
        }

    def _Database__allocate_tab_items(self):
        """Create table ITEMS"""
        self._DictDatabase__database[self._name_tab_items] = {'id': [], 'ind': [], 'key': [], 'child_id': []}

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
            Possible keys are 'id' and 'name'. If key is '!@*', value is a full SQL comparison expression (e.g.,
            `{'!@*': 'ind < 4'}`).

        Returns
        -------
        tuple[row[column]]:
            A tuple containing the selected rows, with each row containing the selected columns in order.
        """
        if self._Database__transaction_level > 0:
            raise NotImplemented('DictDatabase does not currently support transactions')
            # Need to select from the transaction database first and, if not found, selected from the consolidated database.
        else:
            return self._DictDatabase__select_tab(self._DictDatabase__database[self._name_tab_vars], column, where)

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
        if self._Database__transaction_level > 0:
            raise NotImplemented('DictDatabase does not currently support transactions')
            # Need to select from the transaction database first and, if not found, selected from the consolidated database.
        else:
            return self._DictDatabase__select_tab(self._DictDatabase__database[self._name_tab_objs], column, where)

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
        """
        if self._Database__transaction_level > 0:
            raise NotImplemented('DictDatabase does not currently support transactions')
            # Need to select from the transaction database first and, if not found, selected from the consolidated database.
        else:
            return self._DictDatabase__select_tab(self._DictDatabase__database[self._name_tab_items], column, where)

    # Insert
    # ------

    def _Database__insert_tab_vars(self, values):
        """Insert rows into table VARIABLES"""
        if self._Database__transaction_level > 0:  # -> insert in transaction
            self._DictDatabase__insert_tab(self._DictDatabase__database_transaction[self._name_tab_vars], values)
        else:
            self._DictDatabase__insert_tab(self._DictDatabase__database[self._name_tab_vars], values)

    def _Database__insert_tab_objs(self, values):
        """Insert rows into table OBJECTS"""
        if self._Database__transaction_level > 0:  # -> insert in transaction
            self._DictDatabase__insert_tab(self._DictDatabase__database_transaction[self._name_tab_objs], values)
        else:
            self._DictDatabase__insert_tab(self._DictDatabase__database[self._name_tab_objs], values)

    def _Database__insert_tab_items(self, values):
        """Insert rows into table ITEMS"""
        if self._Database__transaction_level > 0:  # -> insert in transaction
            self._DictDatabase__insert_tab(self._DictDatabase__database_transaction[self._name_tab_items], values)
        else:
            self._DictDatabase__insert_tab(self._DictDatabase__database[self._name_tab_items], values)

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
        if self._Database__transaction_level > 0:
            raise NotImplemented('DictDatabase does not currently support transactions')
            # Need to select from the transaction database first and, if not found, selected from the consolidated database.
        else:
            return self._DictDatabase__update_tab(self._DictDatabase__database[self._name_tab_vars], set, where)

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
        if self._Database__transaction_level > 0:
            raise NotImplemented('DictDatabase does not currently support transactions')
            # Need to select from the transaction database first and, if not found, selected from the consolidated database.
        else:
            return self._DictDatabase__update_tab(self._DictDatabase__database[self._name_tab_objs], set, where)

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
        if self._Database__transaction_level > 0:
            raise NotImplemented('DictDatabase does not currently support transactions')
            # Need to select from the transaction database first and, if not found, selected from the consolidated database.
        else:
            return self._DictDatabase__update_tab(self._DictDatabase__database[self._name_tab_items], set, where)

    # Delete
    # ------

    def _Database__delete_tab_vars(self, where):
        """Delete rows from table VARIABLES"""
        if self._Database__transaction_level > 0:
            raise NotImplemented('DictDatabase does not currently support transactions')
            # Need to select from the transaction database first and, if not found, selected from the consolidated database.
        else:
            self._DictDatabase__delete_tab(self._DictDatabase__database[self._name_tab_vars], where)

    def _Database__delete_tab_objs(self, where):
        """Delete rows from table OBJECTS"""
        if self._Database__transaction_level > 0:
            raise NotImplemented('DictDatabase does not currently support transactions')
            # Need to select from the transaction database first and, if not found, selected from the consolidated database.
        else:
            self._DictDatabase__delete_tab(self._DictDatabase__database[self._name_tab_objs], where)

    def _Database__delete_tab_items(self, where):
        """Delete rows from table ITEMS"""
        if self._Database__transaction_level > 0:
            raise NotImplemented('DictDatabase does not currently support transactions')
            # Need to select from the transaction database first and, if not found, selected from the consolidated database.
        else:
            self._DictDatabase__delete_tab(self._DictDatabase__database[self._name_tab_items], where)

    # Transaction/with
    # ----------------

    def _Database__transaction_begin(self):
        """Begin transaction"""
        self._DictDatabase__database_transaction = {
            self._name_tab_vars: {'name': [], 'id': []},
            self._name_tab_objs: {'id': [], 'type': [], 'bool_val': [], 'int_val': [], 'real_val': [], 'txt_val': []},
            self._name_tab_items: {'id': [], 'ind': [], 'key': [], 'child_id': []}
        }

    def _Database_transaction_commit(self):
        """Commit and end transaction"""
        for table_name, table in self._DictDatabase__database.items():
            table.update(self._DictDatabase__database_transaction[table_name])

    def _Database__transaction_rollback(self):
        """Rollback and end transaction"""
        self._DictDatabase__database_transaction = {
            self._name_tab_vars: {'name': [], 'id': []},
            self._name_tab_objs: {'id': [], 'type': [], 'bool_val': [], 'int_val': [], 'real_val': [], 'txt_val': []},
            self._name_tab_items: {'id': [], 'ind': [], 'key': [], 'child_id': []}
        }

    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # DictDatabase
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

    # [DictDatabase] TBD
    # ==================================================================================================================
    # Methods/properties expected to be defined in inheriting classes

    # [DictDatabase; TBD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pass

    # [DictDatabase; TBD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [DictDatabase; TBD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    pass

    # [DictDatabase] FD
    # ==================================================================================================================
    # Methods/properties fully defined here. They can be overloaded by inheriting classes

    # [DictDatabase; FD] Public interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by anything and anyone

    pass

    # [DictDatabase; FD] Private interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used by SQLebra only

    pass

    # [DictDatabase; FD] Protected interface
    # ------------------------------------------------------------------------------------------------------------------
    # To be used internally and by inheriting classes only

    def _DictDatabase__select_tab(self, tab, column, where):
        """Return selected rows and columns from a table

        Parameters
        ----------
        tab: dict
            Table from the database
        column: tuple
            Selected columns to return in order.
        where: dict[column: value]
            Dictionary with the filtering pattern for row selection. If key is '!@*', value is a full SQL comparison
            expression (e.g., `{'!@*': 'ind < 4'}`).
        """
        len_self = len(tab['id'])  # use 'id' (common to all tables) to assert length of table
        if len(where) == 0:  # All rows (no filtering) optimization
            if len(column) == 1:  # Single column optimization.
                if column[0] == 'count(*)':
                    return ((len_self,),)
                elif len_self == 0:
                    return ()
                else:
                    return tuple((c, ) for c in tuple(tab[column[0]]))
            else:
                res_aux = tuple(tab[col] for col in column)
                # Transpose list
                return tuple(map(tuple, zip(*res_aux)))
        # Check table is not empty
        if len_self == 0:
            if len(column) == 1 and column[0] == 'count(*)':
                return ((0,),)
            else:
                return ()
        # Identify selected rows
        ind = self._DictDatabase__identify_rows(tab, len_self, where)
        # Prepare rows
        if len(column) == 1:  # Single column optimization.
            if column[0] == 'count(*)':
                return ((len(ind),),)
            elif len(ind) == 0:
                return ()
            else:
                res = tuple((tab[column[0]][n], ) for n in ind)
        elif len(ind) == 0:
            return ()
        else:
            res = tuple(tuple(tab[col][n] for col in column) for n in ind)
        # Sort before returning if order has not been specified
        if len(ind) > 1:
            if 'key' in tab and not ('key' in where or 'ind' in where):  # -> All items selected, sort results
                aux = tuple((tab['key'][n], tab['ind'][n]) for n in ind)
                sort_ind = sorted(range(len(aux)), key=lambda i: (aux[i][0] or Z, aux[i][1]))
                res = tuple(res[n] for n in sort_ind)
            elif 'name' in tab and not 'name' in where:  # -> All variables selected, sort results
                aux = tuple(tab['name'][n] for n in ind)
                sort_ind = sorted(range(len(aux)), key=lambda i: aux[i])
                res = tuple(res[n] for n in sort_ind)
        return res

    def _DictDatabase__insert_tab(self, tab, values):
        """Insert rows into table

        Parameters
        ----------
        tab: dict
            Database table
        values: dict[column: value]
            Values inserted in the table
        """
        tab_keys = tab.keys()
        values_keys = values.keys()
        if tab_keys != values_keys:
            values_keys = set(values_keys)
            tab_keys = set(tab_keys)
            if values_keys.issubset(tab_keys):  # -> Complete values
                for k in tab_keys - values_keys:
                    values[k] = None
            else:
                raise ValueError(
                    'Table columns ({}) and inserted values ({}) does not match'.format(tab.keys(), values.keys()))
        for k, v in tab.items():
            if isinstance(values[k], (list, tuple)):
                v += values[k]
            else:
                v.append(values[k])

    def _DictDatabase__update_tab(self, tab, set_dict, where):
        """
        Parameters
        ----------
        tab: dict
            Table from the database
        set_dict: dict[column: value]
            Dictionary specifying (column, value) pairs of the updated columns. Column names are keys in the dictionary.
        where: dict[column: value]
            Dictionary specifying (column, value) pairs of the requested rows. Column names are keys in the dictionary.
            If value is a string starting with '!@*', value[3:] is interpreted as a literal SQL query.
        """
        len_self = len(tab['id'])
        if len(where) == 0:  # All rows (no filtering) optimization
            for col, val in set_dict._items():
                tab[col] = [val]*len_self
        # Check table is not empty
        if len_self == 0:
            raise ex.ValueError('No rows identified')
        # Identify selected rows
        ind = self._DictDatabase__identify_rows(tab, len_self, where)
        if len(ind) == 0:
            return
        # Update rows
        for col, val in set_dict.items():
            if isinstance(val, str) and val.startswith('!@*'):
                val = val[3:]
                # Adjust expression, replace column names with references to the table's dictionary
                for c in ('id', 'name', 'type', 'bool_val', 'int_val', 'real_val', 'txt_val', 'ind', 'key', 'child_id'):
                    val = val.replace(c, "tab['{}'][i]".format(c))
                for i in ind:
                    tab[col][i] = eval(val, {'i': i, 'tab': tab})
            else:
                for i in ind:
                    tab[col][i] = val

    def _DictDatabase__delete_tab(self, tab, where):
        """Delete rows from table

        Parameters
        ----------
        tab: dict
            Table from the database
        where: dict[column: value]
            Dictionary with the filtering pattern for row selection. If key is '!@*', value is a full SQL comparison
            expression (e.g., `{'!@*': 'ind < 4'}`).
        """
        if len(where) == 0:  # All rows (no filtering) optimization
            for col in tab.keys():
                tab[col] = []
            return
        # Check table is not empty
        # Identify selected rows
        len_self = len(tab['id'])
        ind = self._DictDatabase__identify_rows(tab, len_self, where)
        if len(ind) == 0:
            return
        # Delete rows
        for col in tab.keys():
            tab[col] = list(tab[col][n] for n in range(len_self) if n not in ind)
            # Slower method
            # for n in sorted(list(compress(range(len(x)), map(operator.not_, item))), reverse=True):
            #     if not item[n]:
            #         del x[n]

    @staticmethod
    def _DictDatabase__identify_rows(tab, len_self, where):
        """
        Identify selected rows

        Parameters
        ----------
        tab: dict
            Table to search
        len_self: int
            Length of table
        where: dict
            Dict with (key, value) pairs corresponding to columns and values. If value is list or tuple, operator `in`
            is used. If key is '!@*', value is a full SQL comparison expression (e.g., `{'!@*': 'ind < 4'}`).

        Returns
        -------
        tuple[bool]:
            Tuple of boolean indices flagging the selected rows
        """
        ind = tuple()
        for col, val in where.items():
            if col == '!@*':  # -> sprecial case
                # Adjust expression
                for c in ('id', 'name', 'type', 'bool_val', 'int_val', 'real_val', 'txt_val', 'ind', 'key', 'child_id'):
                    val = val.replace(c, "tab['{}'][n]".format(c)).replace('NULL', 'None')
                # Run expression
                ind_n = tuple(n
                              for n in range(len_self)
                              if eval(val, {'n': n, 'tab': tab})
                              )
            else:
                ind_n = tuple(n
                              for n, x in enumerate(tab[col])
                              if ((x in val) if isinstance(val, (list, tuple)) else (x == val))
                              )
            if len(ind_n) == 0:
                return ()
            elif len(ind) == 0:
                ind = ind_n
            else:
                ind_n = set(ind_n)
                ind = tuple(n for n in ind if n in ind_n)
            if len(ind) == 0:
                return ()
        return ind



def open_dictdatabase(file, name='sqlebra', mode='+', autocommit=False, file_options=None, **kwargs):
    """Open a DictDatabase by connecting to it and allocating its structure/tables if necessary. The connection
    will remain open until the database is explicitly closed.

    Parameters
    ----------
    file: str
        Full path file name of the DictDatabase file.
    name: str
        Name of the database within `file`
    mode: str
        + 'r' to read an existing file. Raises an error if the file does not exist.
        + 'x' to create a new file. Raises an error if the file exists.
        + 'w' to create a new file, and overwrite it if the file already exists.
        + '+' to open an existing file or create one if it does not exit.
    autocommit: bool, default True
        If True, changes to the database are immediately commited
    **kwargs:
        Additional arguments passed to `pickle.dump`

    See also
    --------
    `Database.close`
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
    if autocommit:
        kwargs['isolation_level'] = 'DEFERRED'
    else:
        kwargs['isolation_level'] = None
    if file_options:
        file = 'file:{}?{}'.format(file, file_options)
    return DictDatabase(file=file, name=name, dump_args=kwargs)
