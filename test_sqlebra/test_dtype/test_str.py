import unittest
import os
from sqlebra.sqlite import SQLiteDB as DB
from sqlebra.dtype import str_ as SQLstr
from sqlebra import exceptions as ex

FILE = 'unittest.sqlebra.db'


class TestInit(unittest.TestCase):

    value = 'test'

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()

    def test_1_set(self):
        self.dbfile['A'] = self.value
        self.assertEqual([(0, 'A', 'str', None, None, None, None, None, self.value, None, 1, 1)],
                         self.dbfile.select(where={'id': 0}))

    def test_2_get(self):
        self.assertIsInstance(self.dbfile['A'], SQLstr)

    def test_3_py(self):
        self.assertEqual(self.value, self.dbfile['A'].py)

    def test_4_edit(self):
        self.dbfile['A'].py = 'edit'
        self.assertEqual('edit', self.dbfile['A'].py)

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
