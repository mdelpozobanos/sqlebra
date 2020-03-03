try:
    import mysql
except ImportError as e:
    print('MySQL package not available. Ignore test.')
    exit()

import unittest
import os
from sqlebra.mysql import MySQLDB as DB
from sqlebra import exceptions as ex


class Test1DBInit(unittest.TestCase):

    file = 'unittest'

    def test_1_open(self):
        DB(self.file, mode='w').open()

    def test_1_reopen(self):
        DB(self.file, mode='w').open()
        DB(self.file, mode='w').open()

    def test_2_connect(self):
        # connect is called within __init__
        dbfile = DB(self.file, mode='w').connect()
        # Trying to connect when it's already connected
        with self.assertRaises(ex.ConnectionError):
            dbfile.connect()
        # Connection objects are set
        self.assertTrue(dbfile._conx,
                        'SQL database file not connected')
        self.assertTrue(dbfile._c,
                        'SQL database file not connected')

    def test_3_disconnect(self):
        dbfile = DB(self.file, mode='w').connect()
        dbfile.disconnect()
        self.assertFalse(dbfile._conx,
                         'SQL database file not disconnected')
        self.assertFalse(dbfile._c,
                         'SQL database file not disconnected')
        # Trying to disconnect again
        with self.assertRaises(ex.ConnectionError):
            dbfile.disconnect()

    def test_4_init_mode_w(self):
        open(self.file, 'a').close()
        DB(self.file, mode='w')
        self.assertFalse(os.path.exists(self.file))

    def test_5_init_mode_r(self):
        with self.assertRaises(FileNotFoundError):
            DB(self.file, mode='r')

    def test_6_init_mode_x(self):
        open(self.file, 'a').close()
        with self.assertRaises(FileExistsError):
            DB(self.file, mode='x')

    @classmethod
    def tearDownClass(cls):
        DB(cls.file, mode='w').open().rm()


class TestDBInterface(unittest.TestCase):

    file = 'unittest'

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(cls.file, mode='w').open()

    def test_1_insert(self):
        self.dbfile.insert(value={'id': 0, 'ind': 10})

    def test_2_select(self):
        row = self.dbfile.select(where={'id': 0})
        self.assertEqual(1, len(row))
        self.assertEqual((0, None, None, None, 10, None, None, None, None, None, None, None), row[0])

    def test_3_delete(self):
        self.dbfile.delete(where={'id': 0})
        row = self.dbfile.select(where={'id': 0})
        self.assertEqual(len(row), 0)

    def text_3_update(self):
        self.dbfile.update(set={'int_val': 2}, where={'id': 1})
        self.assertEqual(self.dbfile.select(column=('int_val', ), where={'id': 1})[0][0], 2,
                         'Value not updated')

    def test_free_id(self):
        self.assertEqual(0, self.dbfile.free_id())

    def test_free_name(self):
        name = self.dbfile.free_name()
        self.assertEqual(len(self.dbfile.select(where={'name': name})), 0,
                         'Existing name returned')

    @classmethod
    def tearDownClass(cls):
        cls.dbfile.rm()


class TestDBTransaction(unittest.TestCase):

    file = 'unittest'

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(cls.file, mode='w').open()

    def test_1(self):
        with self.dbfile.transaction() as db:
            db['A'] = 10
            db['B'] = 11
            db['C'] = 12

    @classmethod
    def tearDownClass(cls):
        cls.dbfile.rm()


if __name__ == '__main__':
    unittest.main()
