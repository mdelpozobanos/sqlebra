import unittest
import os
from sqlebra.sqlite import SQLiteDB as DB
# from sqlebra.mysql import MySQLDB as DB
from sqlebra.dtype import bool_ as SQLbool
from sqlebra import exceptions as ex

if DB.__name__ == 'MySQLDB':
    FILE = 'unittest'
elif DB.__name__ == 'SQLiteDB':
    FILE = 'unittest.sqlebra.db'


class TestInit(unittest.TestCase):

    value = True

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()

    def test_1_set(self):
        self.dbfile['A'] = self.value
        self.assertEqual([(0, 'A', 'bool', None, None, self.value, None, None, None, None, 1, 1)],
                         self.dbfile.select(where={'id': 0}))

    def test_2_get(self):
        self.assertIsInstance(self.dbfile['A'], SQLbool)

    def test_3_py(self):
        self.assertEqual(self.value, self.dbfile['A'].py)

    def test_4_edit(self):
        self.dbfile['A'].py = False
        self.assertEqual(False, self.dbfile['A'].py)

    def test_5_delete(self):
        self.dbfile['A'].delete()
        with self.assertRaises(ex.VariableError):
            self.dbfile['A']

    @classmethod
    def tearDownClass(cls):
        cls.dbfile.rm()


if __name__ == '__main__':
    try:
        unittest.main()
    except Exception as e:
        if os.path.exists(FILE):
            os.remove(FILE)
        raise e
