from ..object._variable import _Variable


class _ObjectFromDatabase:
    """Base class providing an interface to objects from tables"""

    def __init__(self, dbtable):
        self.dbtable = dbtable
        self.db = dbtable.db


class _ObjectFromVariables(_ObjectFromDatabase):
    """Class providing an interface with variables from table VARIABLES"""

    def __getitem__(self, name):
        """Return the selected variable from table VARIABLES.

        Parameters
        ----------
        name: str
            Variable name.

        Returns
        -------
        sqlebra.object:
            Selected variable
        """
        var = _Variable(self.db, name)
        return var._obj

    def __setitem__(self, name, value):
        """Add variable to the database or set value of existing variable.

        Parameters
        ----------
        name: str
            Variable name
        value: Python value
            Variable new value
        """
        # Retrieve object id
        id = self.db._objs.append(value)
        # Check if variable name exists in table VARIABLES
        if name in self.db:
            exists = True
            var = _Variable(self.db, name)
            if id == var._id:
                return
            # Detach variable from object
            var._obj._delete(False)
        else:
            exists = False
        # Retrieve object id
        # id = self._objs.append(value)
        if exists:  # -> update id
            var._id = id
        else:  # -> new variable, insert
            self.db._Database__insert_tab_vars({'name': name, 'id': id})

    def __delitem__(self, name):
        """
        Delete variable from database, taking care of the associated object.

        Parameters
        ----------
        name: str
            Variable name
        """
        var = _Variable(self.db, name)
        id = var._id
        # If `id` is referenced only once (in table VARIABLES or ITEMS), the associated object can be deleted
        num_ref = self.db._obj_ref_count(id=id)
        if num_ref == 1:
            # Let the object delete itself. Objects may have specific deleting processes (e.g. nested objects).
            var._obj.delete()
        # Delete from variable
        self.db._Database__delete_tab_vars(where={'id': id})


class _ObjectFromObjects(_ObjectFromDatabase):
    """Class providing an interface with objects from table OBJECTS"""

    def __getitem__(self, item):
        """Return the selected object(s) from table Objects.

        Parameters
        ----------
        ref: SQlebra.Variable or SQLebra.Item
            The variable or item requesting access to the object(s).
        id: int, tuple, list
            Selected object(s) unique identifier(s)

        Returns
        -------
        sqlebra.object:
            Selected object
        """
        ref, id = item
        rows = self.db._Database__select_tab_objs(column=('id', 'type'), where={'id': id})
        if isinstance(id, (list, tuple)):
            if len(id) > 1:  # Sort rows
                ids = tuple(r[0] for r in rows)
                sort_ind = tuple(map(lambda i: ids.index(i), id))
                if isinstance(ref, (list, tuple)):  # Each object with its reference
                    return tuple(
                        self.db._Database__row2obj(rows[n][0], rows[n][1], ref_n) for ref_n, n in zip(ref, sort_ind)
                    )
                else:  # Same reference for all objects
                    return tuple(
                        self.db._Database__row2obj(rows[n][0], rows[n][1], ref) for n in sort_ind
                    )
            else:  # Return tuple
                if isinstance(ref, (list, tuple)):
                    ref = ref[0]
                return (self.db._Database__row2obj(rows[0][0], rows[0][1], ref), )
        else:
            return self.db._Database__row2obj(rows[0][0], rows[0][1], ref)

    def __setitem__(self, id, value):
        raise NotImplementedError


class _ObjectFromItems(_ObjectFromDatabase):
    """Class providing an interface with items from table ITEMS"""

    def __getitem__(self, item):
        """Return the selected object's item from table Items.

        Parameters
        ----------
        ref: SQlebra.Variable or SQLebra.Item
            The variable or item requesting access to the object(s).
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
        sqlebra.item:
            Selected item
        """
        ref, item = item
        if isinstance(item, int):  # Single item requested
            return self.db._objs.obj[ref._Multi__item_class(ref, ind=item),
                                     self.dbtable[{'id': ref._id, 'ind': item}, 'child_id'][0][0]]
        elif isinstance(item, str):  # Single item requested
            return self.db._objs.obj[ref._Multi__item_class(ref, key=item),
                                     self.dbtable[{'id': ref._id, 'key': item}, 'child_id'][0][0]]
        elif isinstance(item, (tuple, list)):  # Multiple items requested
            if isinstance(item[0], int):
                return self.db._objs.obj[tuple(ref._Multi__item_class(ref, ind=i) for i in item),
                                         tuple(n[0] for n in self.dbtable[{'id': ref._id, 'ind': item}, 'child_id'])]
            elif isinstance(item[0], str):
                return self.db._objs.obj[tuple(ref._Multi__item_class(ref, key=i) for i in item),
                                         tuple(n[0] for n in self.dbtable[{'id': ref._id, 'key': item}, 'child_id'])]
        elif item == slice(None):  # Multiple items requested
            rows = self.dbtable[{'id': ref._id}, ('key', 'ind', 'child_id')]
            return self.db._objs.obj[tuple(ref._Multi__item_class(ref, key=k, ind=i) for k, i, _ in rows),
                                     tuple(r[2] for r in rows)]
        else:  # dict provided
            return self.db._objs.obj[ref, tuple(n[0] for n in self.dbtable[item, 'child_id'])]

    def __setitem__(self, key, value):
        raise NotImplemented
