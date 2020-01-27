import unittest
import os
from sqlebra.sqlite import SQLiteDB as DB
from sqlebra.dtype import int_ as SQLint
from sqlebra import exceptions as ex

FILE = 'unittest.sqlebra.db'


class TestInit(unittest.TestCase):

    value = 10

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()

    def test_1_set(self):
        self.dbfile['A'] = self.value
        self.assertEqual([(0, 'A', 'int', None, None, None, self.value, None, None, None, 1, 1)],
                         self.dbfile.select(where={'id': 0}))

    def test_2_get(self):
        self.assertIsInstance(self.dbfile['A'], SQLint)

    def test_3_py(self):
        self.assertEqual(self.value, self.dbfile['A'].py)

    def test_4_edit(self):
        self.dbfile['A'].py = 20
        self.assertEqual(20, self.dbfile['A'].py)

    def test_5_delete(self):
        self.dbfile['A'].delete()
        with self.assertRaises(ex.VariableError):
            self.dbfile['A']

    @classmethod
    def tearDownClass(cls):
        cls.dbfile.disconnect()
        os.remove(FILE)


if __name__ == '__main__':
    try:
        unittest.main()
    except Exception as e:
        if os.path.exists(FILE):
            os.remove(FILE)
        raise e
