import unittest
import os
from sqlebra.sqlite import SQLiteDB as DB
from sqlebra.python import dict_ as SQLdict
from sqlebra.python import int_ as SQLint
from sqlebra import exceptions as ex

FILE = 'unittest.sqlebra.db'


class TestBaseDB_OverwriteVariable(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()
        cls.dbfile['A'] = 10

    def test_1_to_str(self):
        self.dbfile['A'] = 'test'
        self.assertEqual(1, len(self.dbfile))

    def test_2_to_list(self):
        self.dbfile['A'] = [0, 1, 2]
        self.assertEqual(1, len(self.dbfile))

    def test_3_to_int(self):
        self.dbfile['A'] = 10
        self.assertEqual(1, len(self.dbfile))

    @classmethod
    def tearDownClass(cls):
        cls.dbfile.disconnect()
        os.remove(FILE)


class TestBaseDB_Exist(unittest.TestCase):

    def test_true(self):
        dbfile = DB(FILE, mode='w').open()
        self.assertTrue(dbfile.exists())
        dbfile2 = DB(FILE, mode='+').connect()
        self.assertTrue(dbfile2.exists())

    def test_false(self):
        dbfile = DB(FILE, mode='w').connect()
        self.assertFalse(dbfile.exists())

    def tearDown(self):
        os.remove(FILE)


class TestBaseDB_In(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()
        cls.dbfile['A'] = 10

    def test_true(self):
        self.assertTrue('A' in self.dbfile)

    def test_false(self):
        self.assertFalse('B' in self.dbfile)

    @classmethod
    def tearDownClass(cls):
        cls.dbfile.disconnect()
        os.remove(FILE)


class TestBaseDB_py(unittest.TestCase):

    value = {'a': 'test', 'b': [0, 1, 2], 'c': {'a': 10, 'b': 100}}

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()
        for k, v in cls.value.items():
            cls.dbfile[k] = v

    def test_value(self):
        self.assertTrue(self.value, self.dbfile.py)

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
