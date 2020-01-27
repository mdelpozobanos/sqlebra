from .object import Object
from sqlebra import exceptions as ex


class Nested(Object):
    """
    Parent class defining an object inside an SQL database file with a multiple values
    (i.e. list, tuple or dict).
    """

    # The following variables are defined by the inheriting class.
    col_item = False  # Name of the column containing the indexing item ("key" or "ind")
    pyclass = False  # Python class

    def __init__(self, *args, **kwargs):
        super(Nested, self).__init__(*args, **kwargs)

    def __len__(self):
        return self.db.select(column=['count(*)'], where={'id': self.id, 'root': 0})[0][0]

    def __getitem__(self, item):
        return self.db[{'id': self.id, self.col_item: item}]

    def _check_item_(self, item):
        raise NotImplemented

    def _nameitem_(self, item):
        if isinstance(item, str):
            return "{}['{}']".format(self.name, item)
        else:
            return '{}[{}]'.format(self.name, item)

    def delete(self):
        self.clear()
        self.db.delete({'id': self.id})

    def clear(self):
        # Empty variables
        try:
            nested_rows = self.db[{'id': self.id, 'root': False, 'user_defined': False, '*': 'child_id is not NULL'}][2]
        except ex.VariableError:
            pass
        else:
            for row in nested_rows:
                row.delete()
        self.db.delete({'id': self.id, 'root': False, 'user_defined': False})

    @classmethod
    def value2row(cls, x):
        """
        :param x: Python value to be converted
        :return: (dict) With (key, value) = (column, value) corresponding to a row in the database
            containing the specified value.
        """
        return {'class': cls.pyclass.__name__}
