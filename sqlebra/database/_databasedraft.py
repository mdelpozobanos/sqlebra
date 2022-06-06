"""
_DatabaseDraft
--------------
A _DatabaseDraft defines a draft of rows to be added to the database. It is used during object drafting (see
`Database._draft_obj`) with the aim of combining multiple 'insert' SQL queries into a single one for performance
optimization (at the expense of memory usage).

TableDraft
----------
Class _DatabaseDraft only defines tables OBJECTS and ITEMS. It does so using class `TableDraft`: a wrapper to dict that
includes methods `append` (to insert rows) and `select_obj_id` (to find the `id` of objects).
"""
from .. import exceptions as ex


class _TableDraft(dict):
    """dict of lists with support methods `append` and `_select_obj_id`, used by DatabaseDraft"""

    def append(self, row):
        """Inserts a row to the table draft.

        Parameters
        ----------
        row: dict
            Dictionary specifying (column, value) pairs of the inserted row. Column names are keys in the dictionary.
            If a column is not specified in `row`, a None value will be used.
        """
        if len(row) == 0:
            return
        if isinstance(row['id'], (list, tuple)):
            num_rows = len(row['id'])
            extend_flg = True
        else:
            num_rows = 1
            extend_flg = False
        for k, v in self.items():
            if k in row:
                v2 = row[k]
                if extend_flg:
                    v.extend(v2)
                else:
                    v.append(v2)
            else:
                v.extend((None, )*num_rows)

    def _select_obj_id(self, where):
        """
        Return the object identifiers of the drafted object matching with the specified description.

        Parameters
        ----------
        where: dict
            Dictionary specifying (column, value) pairs of the requested rows. Column names are keys in the dictionary.

        Returns
        -------
        int:
            Selected object identifier. If the object is not found, it raises `sqlebra.exceptions.ObjectError'
        """
        ind = set(range(len(self['id'])))
        for k, v in where.items():
            indN = set(i for i, vi in enumerate(self[k]) if v == vi)
            ind = ind.intersection(indN)
            if len(ind) == 0:
                raise ex.ObjectError(f'No drafted rows matching pattern: {where}')
        if len(ind) > 1:
            raise ex.CorruptedDatabase(f'Multiple drafted rows matching pattern: {where}')
        return tuple(ind)[0]


class _DatabaseDraft:
    """Class holding a draft of a database"""

    def __init__(self, db):
        """
        Parameters
        ----------
        db: `Database`
        """
        self._db = db
        self._objs = None
        self._items = None
        self._clear()

    @property
    def _draft_obj(self):
        """Direct access to database method _draft_obj"""
        return self._db._draft_obj

    def _clear(self):
        """Clears the draft, redefining empty OBJECTS and ITEMS TableDrafts"""
        self._objs = _TableDraft({'id': [], 'type': [], 'bool_val': [], 'int_val': [], 'real_val': [], 'txt_val': []})
        self._items = _TableDraft({'id': [], 'ind': [], 'key': [], 'child_id': []})

    def _free_id(self, num=1):
        """
        Return one or more free drafting object identifier. These are < 0 to differentiate drafted from real object
        identifiers.

        Parameters
        ----------
        num: int
            Number of identifiers requested

        Returns
        -------
        tuple:
            List of requested free identifiers.
        """
        try:
            n0 = self._objs['id'][-1] - 1
        except IndexError:
            n0 = -1
        return tuple(range(n0, n0-num, -1))

    def _select_obj_id(self, objs_dict):
        """
        Return the object identifiers of the object matching with the specified description.

        Parameters
        ----------
        objs_dict: dict
            Dictionary specifying (column, value) pairs of the requested rows. Column names are keys in the dictionary.
            Possible keys are 'id', 'type', 'bool_val', 'int_val', 'real_val' and/or 'txt_val'.

        Returns
        -------
        int:
            Selected object identifier. If the object is not found, it raises `sqlebra.exceptions.ObjectError'
        """
        rows = self._db._objs[objs_dict, 'id']
        if len(rows) == 0:  # -> object not found, check if in draft table OBJECTS
            ind = self._objs._select_obj_id(objs_dict)
            return self._objs['id'][ind]
        else:
            return rows[0][0]

    def _commit(self):
        """
        Consolidates the draft into the database. This includes (1) translating drafted object identifiers to valid
        >0 identifiers for the database, (2) inserting OBJECTS and ITEMS rows in the database and (3) clear the draft.

        Returns
        -------
        int:
            The consolidated version of `id` (i.e. from draft (<0) to final (>=0) object identifier). This is usually
            the object identifier of the top drafted object.
        """
        # Replace drafted ids (i.e., ids<0) with real ids
        if len(self._objs['id']) > 0:
            ids = self._db._free_id(abs(self._objs['id'][-1]))
            self._objs['id'] = tuple(ids[id] if id < 0 else id for id in self._objs['id'])
            self._items['id'] = tuple(ids[id] if id < 0 else id for id in self._items['id'])
            self._items['child_id'] = tuple(ids[id] if id < 0 else id for id in self._items['child_id'])
            # Insert objects to database
            self._db._Database__insert_tab_objs(self._objs)
        else:
            ids = {}
        # Insert items to database
        if len(self._items['id']) > 0:
            self._db._Database__insert_tab_items(self._items)
        # Clear draft
        self._clear()
        # Return translated id
        return ids
