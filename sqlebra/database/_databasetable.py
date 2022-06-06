from ._objectfromdatabase import _ObjectFromVariables, _ObjectFromObjects, _ObjectFromItems
from ..object.object_ import object_


class _DatabaseTable:
    """Base class providing an interface with a table in the database"""

    def __init__(self, db):
        self.db = db
        # To be defined by the inheriting class
        self.obj = None
        self._DatabaseTable_select_tab = None
        self._DatabaseTable_delete_tab = None

    def __contains__(self, rows):
        """Check if the table contains matching rows

        Parameters
        ----------
        rows: dict
            Dict where keys are column names and values are the selected column values

        Returns
        -------
        bool:
            True if matching rows are found
        """
        return self._DatabaseTable_select_tab(column=('count(*)', ), where=rows)[0][0] > 0

    def __len__(self):
        """Number of rows in the table"""
        return self._DatabaseTable_select_tab(column=('count(*)', ))[0][0]

    def __getitem__(self, *args):
        """Select rows from table

        Parameters
        ----------
        rows: dict
            Selected rows
        columns: str or tuple [optional]
            Selected columns for returning. If str, a single column is return. If tuple, a tuple of length
            `len(columns)` will be returned with the selected columns. If not specified, all columns will be returned.

        Returns
        -------
        tuple
            Selected rows and columns
        """
        args = args[0]  # __getitem__ always receive a single argument
        if len(args) == 1:
            where = args
            column = slice(None)  # Return all columns
        else:
            where, column = args
            if where == slice(None):  # Return all columns
                where = {}
            if isinstance(column, str):  # Single column
                column = (column,)
        return self._DatabaseTable_select_tab(column=column, where=where)

    def __setitem__(self, rows, value):
        """Update rows in table

        Parameters
        ----------
        rows: dict
            Selected rows
        value: dict
            Dict where keys are column names and value are updated values.

        Returns
        -------
        tuple
            Selected rows
        """
        self._DatabaseTable_update_tab(set=value, where=rows)

    def __delitem__(self, where):
        """Delete rows from table

        Parameters
        ----------
        where: dict
            Selected rows
        """
        self._DatabaseTable_delete_tab(where=where)


class _DatabaseTableVariables(_DatabaseTable):
    """Class representing table VARIABLES"""

    def __init__(self, db):
        super().__init__(db)
        self.obj = _ObjectFromVariables(self)
        self._DatabaseTable_select_tab = db._Database__select_tab_vars
        self._DatabaseTable_delete_tab = db._Database__delete_tab_vars
        self._DatabaseTable_update_tab = db._Database__update_tab_vars

    # overloaded method
    def __contains__(self, item):
        """Return true if the specified row or variable exist in the database

        Parameters
        ----------
        item: str or dict
            Dict where keys are column names and values are the selected column values

        Returns
        -------
        bool:
            True if a matching variable or rows are found
        """
        if isinstance(item, str):
            return super().__contains__({'name': item})
        else:
            return super().__contains__(item)


class _DatabaseTableObjects(_DatabaseTable):
    """Class representing table OBJECTS"""

    def __init__(self, db):
        super().__init__(db)
        self.obj = _ObjectFromObjects(self)
        self._DatabaseTable_select_tab = db._Database__select_tab_objs
        self._DatabaseTable_delete_tab = db._Database__delete_tab_objs
        self._DatabaseTable_update_tab = db._Database__update_tab_objs

    def append(self, value):
        """
        Add object `value` to the database and return its assigned identifier. If `value` already exists in the
        database, simply return its object identifier.

        Parameters
        ----------
        value: `python variable`
            Python variable to add to tables OBJECTS and ITEMS as required.

        Returns
        -------
        int:
            Identifier of the inserted or existing object.
        """
        if isinstance(value, object_):  # -> value is an SQLebra object
            if value.db == self:  # -> value is stored in this database, just point to it
                return value._id
            else:  # -> value stored in a different database.
                value = value.py
        # An object may need to be created
        id = self.db._draft_obj(value)
        if id >= 0:  # A possitive root id means the object exists and there is nothing to commit
            return id
        else:
            return self.db._commit_draft()[id]


class _DatabaseTableItems(_DatabaseTable):
    """Class representing table ITEMS"""

    def __init__(self, db):
        super().__init__(db)
        self.obj = _ObjectFromItems(self)
        self._DatabaseTable_select_tab = db._Database__select_tab_items
        self._DatabaseTable_delete_tab = db._Database__delete_tab_items
        self._DatabaseTable_update_tab = db._Database__update_tab_items

    def __contains__(self, item):
        """Return true if the specified item exist in the database

        Parameters
        ----------
        item: dict
            Dict with keys:
            `id`: int
                Object unique identifier
            `key`: str [Optional]
                Item's key
            `ind`: int [Optional]
                Item's ind

        Returns
        -------
        bool:
            True if the specified items exists in table ITEMS.
        """
        return self.db._Database__select_tab_items(column=('count(*)',), where=item)[0][0] == 1

    def __len__(self):
        """Number of items in the database"""
        return self.db._Database__select_tab_items(column=('count(*)',))[0][0]
