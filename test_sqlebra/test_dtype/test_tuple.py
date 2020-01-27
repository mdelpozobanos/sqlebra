import unittest
import os
from sqlebra.sqlite import SQLiteDB as DB
from sqlebra.dtype import tuple_ as SQLtuple

FILE = 'unittest.sqlebra.db'


class TestInit(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()

    def test_1_set(self):
        self.dbfile['A'] = (10, 11, 12)
        self.assertEqual([None, 10, 11, 12], [r[6] for r in self.dbfile.select(where={'id': 0})])

    def test_2_get(self):
        self.assertIsInstance(self.dbfile['A'], SQLtuple)

    def test_3_edit(self):
        with self.assertRaises(TypeError):
            self.dbfile['A'][0] = 10

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
