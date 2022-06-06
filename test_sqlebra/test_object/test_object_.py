import os
import math
from sqlebra.database.dictdatabase import open_dictdatabase
from sqlebra.database.sqlite import open_sqlitedatabase
from test_sqlebra.test_init import Test_SQLebraObject
from copy import copy


class DatabaseDict:

    def __init__(self, test_base):
        self.test_base = test_base

    def varname(self, name):
        if isinstance(name, int):
            return 'x[{}]'.format(str(name))
        else:
            return name

    def __getitem__(self,  name):
        return self.test_base.db[self.varname(name)]

    def __setitem__(self, name, value):
        self.test_base.db[self.varname(name)] = value


class Test_object_(Test_SQLebraObject):

    def setUp(self):
        self.dbo = DatabaseDict(self)

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._steps_list += ('init', 'pyclass', 'info',
                            'py_get', 'py_set',
                            'delete', 'restore_delete',
                            'eq', 'ne', 'isNone',
                            'str', 'bool',
                            'transaction'
                            )
        # Databases
        cls.file = os.path.join(os.path.dirname(__file__), '..', 'files', cls.__name__ + '.mem')
        cls._try_remove(cls.file)
        cls.database = {
            'DictDatabase': open_dictdatabase(file=cls.file, name='sqlebra', mode='+'),
            'SQLiteDatabase': open_sqlitedatabase(file=cls.file, name='sqlebra', mode='+', autocommit=False)
        }
        cls.dbname = None
        cls.obj_list = {'SQLiteDatabase': None}
        # Other variables
        cls.SQLebraClass = None
        cls.PyClass = None
        cls.x = []
        cls.xr = None  # Running value
        cls.dbo = None

    @classmethod
    def tearDownClass(cls) -> None:
        cls._try_remove(cls.file)

    @property
    def db(self):
        return self.database[self.dbname]

    # Test public interface
    # ------------------------------------------------------------------------------------------------------------------

    def init(self):
        for n, x_n in enumerate(self.x):
            self.dbo[n] = x_n
        # Running variable
        if hasattr(self.x[0], 'copy'):
            self.xr = self.x[0].copy()
            self.dbo['xr'] = self.x[0].copy()
        else:
            self.xr = copy(self.x[0])
            self.dbo['xr'] = copy(self.x[0])
        # Reset
        self._been_here = []

    def pyclass(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(self.PyClass, type(x_n))
            self.assertEqual(self.PyClass, self.dbo[0].pyclass)

    def info(self):
        self.assertEqual(
            '{}[sqlebra.{}:id{}]'.format(self.db['xr'].db.info, self.db['xr'].pyclass.__name__, self.db['xr']._id),
            self.db['xr'].info
        )

    def py_get(self):
        for n, x_n in enumerate(self.x):
            if isinstance(x_n, float) and math.isnan(x_n):
                self.assertTrue(math.isnan(self.dbo[n].py))
            else:
                self.assertEqual(x_n, self.dbo[n])

    def py_set(self):
        assert self.xr == self.dbo['xr']
        with self.subTest(with_='new value'):
            if hasattr(self.x[0], 'copy'):
                self.xr = self.x[1].copy()
            else:
                self.xr = copy(self.x[1])
            self.dbo['xr'].py = self.x[1]
            self.assertEqual(self.xr, self.dbo['xr'])
        with self.subTest(with_='itself'):
            self.dbo['xr'].py = self.dbo['xr']
            self.assertEqual(self.xr, self.dbo['xr'])
        with self.subTest(with_='with SQLebra object'):
            if hasattr(self.x[0], 'copy'):
                self.xr = self.x[0].copy()
            else:
                self.xr = copy(self.x[0])
            self.dbo['xr'].py = self.dbo[0]
            self.assertEqual(self.xr, self.dbo['xr'])

    def delete(self):
        for n in range(len(self.x)):
            self.dbo[n].delete()
            with self.assertRaises(IndexError):
                self.dbo[n]

    def restore_delete(self):
        for n, x in enumerate(self.x):
            self.dbo[n] = x

    def copy2file(self, file):
        raise NotImplementedError

    # Operators
    # ---------

    def eq(self):
        for n, x_n in enumerate(self.x):
            if isinstance(x_n, float) and math.isnan(x_n):
                self.assertTrue(math.isnan(self.dbo[n]))
            else:
                self.assertEqual(x_n, self.dbo[n])

    def ne(self):
        self.assertNotEqual(self.dbo[0].py, self.dbo[1])

    def isNone(self):
        for n, x_n in enumerate(self.x):
            if x_n is None:
                self.assertTrue(self.dbo[n].isNone())
            else:
                self.assertFalse(self.dbo[n].isNone())

    # Casting
    # -------

    def str(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(str(x_n), str(self.dbo[n]))

    def bool(self):
        """Cast to bool"""
        for n, x_n in enumerate(self.x):
            self.assertEqual(bool(x_n), bool(self.dbo[n]))

    # Database direct access
    # ----------------------

    def transaction(self):
        return
        # TODO: Fix this
        self.dbo['xr'] = self.dbo[0].py
        try:
            with self.dbo['xr']:
                self.dbo['xr'] = self.dbo[1]
                raise ValueError('Expected')
        except ValueError as exc:
            if exc.args[0] != 'Expected':
                raise exc
        self.assertEqual(self.dbo[0], self.dbo['xr'])

    def _run_steps(self):
        for self.dbname in self.database.keys():
            with self.subTest(database=self.dbname):
                super()._run_steps()