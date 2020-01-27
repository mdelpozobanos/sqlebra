import unittest
import os
from sqlebra.sqlite import SQLiteDB as DB
from sqlebra.dtype import NoneType_ as SQLNone

FILE = 'unittest.sqlebra.db'


class TestInit(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()

    def test_1_set(self):
        self.dbfile['A'] = None
        self.assertEqual([(0, 'A', 'NoneType', None, None, None, None, None, None, None, 1, 1)],
                         self.dbfile.select(where={'id': 0}))

    def test_2_get(self):
        self.assertIsInstance(self.dbfile['A'], SQLNone)

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
