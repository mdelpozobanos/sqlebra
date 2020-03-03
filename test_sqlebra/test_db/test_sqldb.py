import unittest
import os
from sqlebra.sqlite import SQLiteDB
from sqlebra.mysql import MySQLDB
from sqlebra import exceptions as ex

FILE = 'unittest.db'


class TestDB(unittest.TestCase):

    def check_db(self, dbfile, num_vars, num_objs, num_items, msg):
        self.assertEqual(num_vars, dbfile.select(dbfile.tab_vars, column=('count(*)',))[0][0],
                         msg=msg.format('Bad variables table'))
        self.assertEqual(num_objs, dbfile.select(dbfile.tab_objs, column=('count(*)',))[0][0],
                         msg=msg.format('Bad objects table'))
        self.assertEqual(num_items, dbfile.select(dbfile.tab_items, column=('count(*)',))[0][0],
                         msg=msg.format('Bad items table'))


def test_generator():

    def _test_generator(db):

        def test(self):

            msg = '[{}] {{}}. Test aborted'.format(db.__name__)

            try:

                # Allocate database object
                dbfile = db(FILE, mode='w')

                # Connect
                dbfile.connect()
                if db is SQLiteDB:
                    self.assertTrue(os.path.exists(FILE), msg.format('File not created'))
                self.assertTrue(dbfile._conx, msg.format('Bad _conx'))
                self.assertTrue(dbfile._c, msg.format('Bad _c'))

                # Trying to connect when it's already connected
                with self.assertRaises(ex.ConnectionError):
                    dbfile.connect()

                # Exist
                self.assertFalse(dbfile.exists())

                # Initialize
                dbfile.init()
                self.assertTrue(dbfile.exists())

                # Diconnect
                dbfile.disconnect()
                self.assertFalse(dbfile._conx, 'SQL database file not disconnected')
                self.assertFalse(dbfile._c, 'SQL database file not disconnected')

                # Trying to disconnect when it's already disconnected
                with self.assertRaises(ex.ConnectionError):
                    dbfile.disconnect()

                # Open database
                dbfile.open()

                # Re-open database
                with self.assertRaises(ex.ConnectionError):
                    dbfile.open()

                # Clear database
                dbfile.clear()
                self.assertFalse(dbfile.exists())

                # Init modes
                open(FILE, 'a').close()
                db(FILE, mode='w')
                self.assertFalse(os.path.exists(FILE))

                with self.assertRaises(FileNotFoundError):
                    db(FILE, mode='r')

                open(FILE, 'a').close()
                with self.assertRaises(FileExistsError):
                    db(FILE, mode='x')

            finally:
                if dbfile.exists():
                    dbfile.clear()
                    dbfile.commit()
                if os.path.exists(FILE):
                    os.remove(FILE)

        # Adjust name
        test.__name__ = '{}_{}'.format(test.__name__, db.__name__)
        return test


    def _test_interface_generator(db):

        def test(self):
            msg = '[{}] {{}}. Test aborted'.format(db.__name__)

            # Set up
            dbfile = db(FILE, mode='w').open()

            try:

                # Insert in...
                # ... tab_vars
                dbfile.insert(dbfile.tab_vars, value={'name': 'test', 'id': 0})
                # ... tab_objs
                dbfile.insert(dbfile.tab_objs, value={'id': 0, 'type': 'list'})
                # ... tab_items
                dbfile.insert(dbfile.tab_items, value={'id': 0, 'ind': 0, 'child_id': 1})
                self.check_db(dbfile, 1, 1, 1, msg=msg)

                # Select from...
                # ... tab_vars
                row = dbfile.select(dbfile.tab_vars, where={'id': 0})
                self.assertEqual(1, len(row), msg.format('Bad variables table row'))
                self.assertEqual(('test', 0), row[0], msg.format('Bad variables table row'))
                # ... tab_objs
                row = dbfile.select(dbfile.tab_objs, where={'id': 0})
                self.assertEqual(1, len(row), msg.format('Bad objects table row'))
                self.assertEqual((0, 'list', None, None, None, None), row[0], msg.format('Bad objects table row'))
                # ... tab_items
                row = dbfile.select(dbfile.tab_items, where={'id': 0})
                self.assertEqual(1, len(row), msg.format('Bad items table row'))
                self.assertEqual((0, None, 0, 1), row[0], msg.format('Bad items table row'))

                # Insert multiple...
                dbfile.insert(dbfile.tab_objs, value={'id': (1, 4, 7, 8, 9),
                                                      'type': ('int', 'int', 'int', 'int', 'int'),
                                                      'int_val': (10, 11, 12, 13, 14)})
                dbfile.insert(dbfile.tab_items, value={'id': (0, 0, 0, 0),
                                                       'ind': (1, 2, 3, 4),
                                                       'child_id': (4, 7, 8, 9)})
                self.check_db(dbfile, 1, 6, 5, msg=msg)

                # Update
                dbfile.update(dbfile.tab_objs, set={'int_val': 100}, where={'id': 1})
                self.assertEqual(100, dbfile.select(dbfile.tab_objs, column=('int_val',), where={'id': 1})[0][0],
                                 msg.format('Value not updated'))

                # Delete
                dbfile.delete(dbfile.tab_objs, where={'id': 9})
                dbfile.delete(dbfile.tab_items, where={'id': 0, 'child_id': 9})
                self.check_db(dbfile, 1, 5, 4, msg=msg)
                self.assertEqual(0, len(dbfile.select(dbfile.tab_objs, where={'id': 9})),
                                 msg.format('Bad row deleted'))
                self.assertEqual(0, len(dbfile.select(dbfile.tab_items, where={'id': 0, 'child_id': 9})),
                                 msg.format('Bad row deleted'))

                # Free id
                self.assertEqual(9, dbfile.free_id()[0])
                self.assertEqual(2, dbfile.free_id(compact=True)[0])
                self.assertEqual(tuple(range(9, 19)), dbfile.free_id(10),
                                 msg.format('Bad 10 free id'))
                self.assertEqual((2, 3, 5, 6, 9, 10, 11, 12, 13, 14), dbfile.free_id(10, compact=True),
                                 msg.format('Bad consecutive 10 free id'))

            finally:
                if dbfile.exists():
                    dbfile.clear()
                    dbfile.commit()
                if os.path.exists(FILE):
                    os.remove(FILE)

        # Adjust name
        test.__name__ = '{}_{}_interface'.format(test.__name__, db.__name__)
        return test

    return tuple((_test_generator(db) for db in (SQLiteDB, MySQLDB))) + \
           tuple((_test_interface_generator(db) for db in (SQLiteDB, MySQLDB)))


class TestDBInterface():

    file = 'unittest.sqlebra.db'

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(cls.file, mode='w').open()

    def tearDown(self):
        self.dbfile.commit()

    @classmethod
    def tearDownClass(cls):
        cls.dbfile.disconnect()
        del cls.dbfile
        os.remove(cls.file)


class TestDBTransaction():

    file = 'unittest.sqlebra.db'

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(cls.file, mode='w').open()

    def test_1(self):
        with self.dbfile.transaction() as db:
            db.insert(self.dbfile.tab_objs, value={'id': 0, 'type': 'int', 'int_val': 1})

    @classmethod
    def tearDownClass(cls):
        cls.dbfile.disconnect()
        os.remove(cls.file)


# Contruct tests
for test in test_generator():
    setattr(TestDB, test.__name__, test)


if __name__ == '__main__':
    unittest.main()
