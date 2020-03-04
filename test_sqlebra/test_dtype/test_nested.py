import os
import operator
import copy
import unittest
import numpy as np

from sqlebra.sqlite import SQLiteDB as DB
# from sqlebra.mysql import MySQLDB as DB
from sqlebra import exceptions as ex
from sqlebra.python import list_ as SQLlist
from sqlebra.python import tuple_ as SQLtuple
from sqlebra.python import dict_ as SQLdict

# Define
py_class = (list, tuple, dict)
MAIN_TEST = {}
for py_c in py_class:
    MAIN_TEST[py_c] = True

if DB.__name__ == 'MySQLDB':
    FILE = 'unittest'
elif DB.__name__ == 'SQLiteDB':
    FILE = 'unittest.sqlebra.db'


class TestNested(unittest.TestCase):

    def check_db(self, dbfile, num_vars, num_objs, num_items):
        self.assertEqual(num_vars, dbfile.select(dbfile.tab_vars, column=('count(*)',))[0][0],
                         msg='Bad variables table. Test aborted')
        self.assertEqual(num_objs, dbfile.select(dbfile.tab_objs, column=('count(*)',))[0][0],
                         msg='Bad objects table. Test aborted')
        self.assertEqual(num_items, dbfile.select(dbfile.tab_items, column=('count(*)',))[0][0],
                         msg='Bad items table. Test aborted')


def test_generator(py_class):
    # Define variables
    if py_class is list:
        sql_class = SQLlist
        py_v1 = list(range(0, 10))
        py_v2 = np.arange(0.0, 0.4, 0.1).tolist()
        item1 = 0
        item2 = -2
        slice1 = slice(item1, item2)
        # Triple nested object
        py_v3 = list(range(3))
        py_v3[-1] = py_v1.copy()
        py_v3[-1][-1] = py_v2.copy()
        item3 = -1
    elif py_class is tuple:
        sql_class = SQLtuple
        py_v1 = tuple(range(0, 10))
        py_v2 = tuple(np.arange(0.0, 0.4, 0.1).tolist())
        item1 = 0
        item2 = -2
        slice1 = slice(item1, item2)
        # Triple nested object
        py_v3 = list(range(3))
        py_v3[-1] = list(py_v1)
        py_v3[-1][-1] = py_v2
        py_v3[-1] = tuple(py_v3[-1])
        py_v3 = tuple(py_v3)
        item3 = -1
    elif py_class is dict:
        sql_class = SQLdict
        py_v1 = {'a': 1, 'b': 1.1, 'c': True, 'd': 'val1', 'e': None}
        py_v2 = {'a': 2, 'b': 2.2, 'c': False, 'd': 'val2', 'e': None}
        item1 = 'a'
        item2 = 'd'
        slice1 = ('a', 'c', 'e')
        # Triple nested object
        py_v3 = {'a': 1, 'b': 'triple', 'c': True}
        py_v3['e'] = py_v1.copy()
        py_v3['e']['e'] = py_v2.copy()
        item3 = 'e'
    items = [1, 1.0, 'a', None, True, 1]

    # test function
    def test(self):

        # Set up
        dbfile = DB(FILE, mode='w').open()

        try:

            # test instantiate
            dbfile['x'] = py_v1
            self.check_db(dbfile, 1, len(py_v1) + 1, len(py_v1))

            # test get
            x = dbfile['x']
            self.assertIsInstance(x, sql_class,
                                  msg='Wrong SQLebra variable type retrieved. Test aborted')

            # test py
            xpy = x.py
            self.assertIsInstance(xpy, py_class,
                                  msg='Wrong variable type retrieved. Test aborted')
            self.assertEqual(py_v1, xpy,
                             msg='Wrong object value retrieved. Test aborted')

            # Indexing -----------------------------------------------------------------

            # test get item
            self.assertEqual(py_v1[item1], x[item1].py,
                             msg='Wrong item {} retrieved. Test aborted'.format(item1))
            self.assertEqual(py_v1[item2], x[item2].py,
                             msg='Wrong item {} retrieved. Test aborted'.format(item2))

            # Clearing  -----------------------------------------------------------------

            # Test clear
            x.clear()
            self.check_db(dbfile, 1, 1, 0)
            self.assertEqual(0, len(x),
                             msg='Wrong variable length after clear. Test aborted')
            self.assertEqual(py_class(), x.py,
                             msg='Wrong variable after clear. Test aborted')

            # Test py set
            id = x.id
            x.py = py_v1
            self.check_db(dbfile, 1, len(py_v1) + 1, len(py_v1))
            self.assertEqual(id, x.id,
                             msg='SQL-object id not recycled. Test aborted')
            self.assertEqual(py_v1, x.py,
                             msg='SQL-object value not updated. Test aborted')

            # Editing -----------------------------------------------------------

            # Test py edit
            id = x.id
            x.py = py_v2
            self.check_db(dbfile, 1, len(py_v2) + 1, len(py_v2))
            self.assertEqual(id, x.id,
                             msg='SQL-object id not recycled. Test aborted')
            self.assertEqual(py_v2, x.py,
                             msg='SQL-object value not updated. Test aborted')

            if py_class is tuple:

                with self.assertRaises(TypeError):
                    x[item1] = py_v1[item1]

            else:

                # test edit item
                py_v2[item1] = py_v1[item1]
                x[item1] = py_v1[item1]
                self.check_db(dbfile, 1, len(py_v2) + 1, len(py_v2))
                self.assertEqual(py_v2[item1], x[item1].py,
                                 msg='SQL-object item not updated. Test aborted')

                # test edit item, set with SQLebra object
                py_v2[item1] = py_v2[item2]
                x[item1] = x[item2]
                self.check_db(dbfile, 1, len(py_v2), len(py_v2))
                self.assertEqual(x[item1].py, x[item2].py,
                                 msg='SQL-object item not updated. Test aborted')

                # test implicit change of item class
                for i in items:
                    py_v2[item1] = i
                    x[item1] = i
                    self.assertIsInstance(x[item1].py, type(py_v2[item1]),
                                          msg='SQL-object item class not updated. Test aborted')
                    self.assertEqual(py_v2[item1], x[item1].py,
                                     msg='SQL-object item value not updated. Test aborted')
                    self.check_db(dbfile, 1, len(py_v2) + 1, len(py_v2))

                # Using existing items
                py_v2[item1] = py_v2[item2]
                x[item1] = x[item2]
                self.assertIsInstance(x[item1].py, type(py_v2[item1]),
                                      msg='SQL-object item class not updated. Test aborted')
                self.assertEqual(py_v2[item1], x[item1].py,
                                 msg='SQL-object item value not updated. Test aborted')
                self.check_db(dbfile, 1, len(py_v2), len(py_v2))

            # Slicing ------------------------------------------------

            if py_class is not dict:
                # test slice
                py_s = py_v2[slice1]
                s = x[slice1]
                self.assertEqual(len(py_s), len(s),
                                 msg='Wrong slice length. Test aborted')
                self.assertEqual(py_s, s.py,
                                 msg='Wrong slice retrieved. Test aborted')

                # test index slice
                self.assertEqual(py_s[item1], s[item1].py,
                                 msg='Wrong slice item retrieved. Test aborted')
                self.assertEqual(py_s[item2], s[item2].py,
                                 msg='Wrong slice negative item retrieved. Test aborted')

            # Deleting ------------------------------------------------

            # Test delete item
            if py_class is tuple:
                with self.assertRaises(TypeError):
                    del x[item1]
            else:
                del py_v2[item1]
                del x[item1]
                self.check_db(dbfile, 1, len(py_v2) + 1, len(py_v2))
                self.assertEqual(py_v2, x.py,
                                 msg='Error deleting item. Test aborted')

            # Test delete
            x.delete()
            with self.assertRaises(ex.VariableError,
                                   msg='Error deleting variable. Test aborted'):
                dbfile['x']
            with self.assertRaises(ex.ObjectError,
                                   msg='Error deleting object. Test aborted'):
                dbfile[0]
            self.check_db(dbfile, 0, 0, 0)

            # Test delete with multiple items pointing to the same value
            if py_class is not tuple:
                dbfile['x'] = py_v1
                x = dbfile['x']
                x[item1] = x[item2]
                x.delete()
                with self.assertRaises(ex.VariableError,
                                       msg='Error deleting variable. Test aborted'):
                    dbfile['x']
                with self.assertRaises(ex.ObjectError,
                                       msg='Error deleting object. Test aborted'):
                    dbfile[0]
                self.check_db(dbfile, 0, 0, 0)

            # Nested-nested ------------------------------------------------

            dbfile['x'] = py_v3
            x = dbfile['x']
            self.check_db(dbfile, 1,
                          1 + len(py_v3) + len(py_v3[item3]) + len(py_v3[item3][item3]),
                          len(py_v3) + len(py_v3[item3]) + len(py_v3[item3][item3]))
            self.assertEqual(len(py_v3), len(x),
                             msg='Wrong length. Test aborted')
            x_py = x.py
            self.assertEqual(py_v3, x_py,
                             msg='Wrong py value. Test aborted')

            # Edit the nested item
            if py_class is not tuple:
                py_v3[item3][item3] = py_v1
                x[item3][item3] = py_v1
                self.check_db(dbfile, 1,
                              1 + len(py_v3) + len(py_v3[item3]) + len(py_v3[item3][item3]),
                              len(py_v3) + len(py_v3[item3]) + len(py_v3[item3][item3]))
                self.assertEqual(len(py_v3), len(x),
                                 msg='Wrong length. Test aborted')
                x_py = x.py
                self.assertEqual(py_v3, x_py,
                                 msg='Wrong py value. Test aborted')

            if py_class is dict:  # Implicit creation of key
                py_v3['implicit'] = 100
                x['implicit'] = 100
                self.check_db(dbfile, 1,
                              1 + len(py_v3) + len(py_v3[item3]) + len(py_v3[item3][item3]),
                              len(py_v3) + len(py_v3[item3]) + len(py_v3[item3][item3]))
                self.assertEqual(len(py_v3), len(x),
                                 msg='Wrong length. Test aborted')
                self.assertEqual(py_v3, x.py,
                                 msg='Wrong py value. Test aborted')

            # Clear
            x.clear()
            self.check_db(dbfile, 1, 1, 0)
            self.assertEqual(0, len(x),
                             msg='Wrong variable length after clear. Test aborted')
            self.assertEqual(py_class(), x.py,
                             msg='Wrong variable after clear. Test aborted')

            # Delete
            dbfile['x'] = py_v3
            x = dbfile['x']
            x.delete()
            self.check_db(dbfile, 0, 0, 0)

        except:
            MAIN_TEST[py_class] = False
            raise
        finally:  # Be clean
            dbfile.rm()

    # Adjust name
    test.__name__ = '{}_{}_main'.format(test.__name__, py_class.__name__)

    return test


def test_extra_generator(py_class):
    # Define variables
    if py_class is list:
        sql_class = SQLlist
        py_v1 = list(range(0, 4))
        py_v2 = np.arange(0.0, 0.4, 0.1).tolist()
        v1 = 2
        v2 = ['a', 'b', 'c']
        item1 = 0
        item2 = -1
        slice1 = slice(item1, item2)
    elif py_class is tuple:
        sql_class = SQLtuple
        py_v1 = tuple(range(0, 10))
        py_v2 = tuple(np.arange(0.0, 0.4, 0.1).tolist())
        v1 = 2
        v2 = ['a', 'b', 'c']
        item1 = 0
        item2 = -1
        slice1 = slice(item1, item2)
    elif py_class is dict:
        sql_class = SQLdict
        py_v1 = {'a': 1, 'b': 1.1, 'c': True, 'd': 'val1', 'e': None}
        py_v2 = {'a': 2, 'b': 2.2, 'c': False, 'd': 'val2', 'e': None}
        v1 = 1.1
        v2 = {'1': 1, '2': 2}
        item1 = 'a'
        item2 = 'e'
        slice1 = ('a', 'c', 'e')

    def test_iterator_generator():

        def test(self):

            self.assertTrue(MAIN_TEST[py_class],
                            msg='Main test not past. Ignore specific tests')

            # Set up
            dbfile = DB(FILE, mode='w').open()
            dbfile['x1'] = py_v1
            x1 = dbfile['x1']

            try:
                # test iter
                if py_class is dict:

                    # test iter
                    x_iter = x1.__iter__()
                    for v1, v2 in zip(py_v1, x_iter):
                        self.assertEqual(v1, v2,
                                         msg='iterator error')

                    # test keys
                    x_iter = x1.keys()
                    for v1, v2 in zip(py_v1.keys(), x_iter):
                        self.assertEqual(v1, v2,
                                         msg='keys error')

                    # test values
                    x_iter = x1.values()
                    for v1, v2 in zip(py_v1.values(), x_iter):
                        self.assertEqual(v1, v2.py,
                                         msg='values error')

                else:
                    for v1, v2 in zip(py_v1, x1):
                        self.assertEqual(v1, v2.py)
            finally:
                # Be clean
                dbfile.rm()

        # Adjust name
        test.__name__ = '{}_{}_iterator'.format(test.__name__, py_class.__name__)

        return test

    def test_fcn_generator(fcn):

        def test(self):

            self.assertTrue(MAIN_TEST[py_class],
                            msg='Main test not past. Ignore specific tests')

            # Set up
            dbfile = DB(FILE, mode='w').open()
            dbfile['x1'] = py_v1
            v1 = copy.copy(py_v1)
            x1 = dbfile['x1']

            try:  # Apply method
                self.assertEqual(fcn(v1), fcn(x1))
                self.assertEqual(v1, x1.py)
            finally:  # Be clean
                dbfile.rm()

        # Adjust name
        if isinstance(fcn, str):
            test.__name__ = '{}_{}'.format(test.__name__, fcn)
        else:
            test.__name__ = '{}_{}'.format(test.__name__, fcn.__name__)
        return test

    def _fcn(name):
        def test(x):
            return x.__getattribute__(name)()
        test.__name__ = name
        return test
    sort_fcn = _fcn('sort')
    reverse_fcn = _fcn('reverse')
    copy_fcn = _fcn('copy')

    def _fcn_x(fcn):
        def test(x):
            return fcn(x)
        test.__name__ = fcn.__name__
        return test
    any_fcn = _fcn_x(any)
    min_fcn = _fcn_x(min)
    max_fcn = _fcn_x(max)

    def _fcn_v1(name):
        def test(x):
            return x.__getattribute__(name)(v1)
        test.__name__ = name
        return test
    count_fcn = _fcn_v1('count')
    index_fcn = _fcn_v1('index')
    append_fcn = _fcn_v1('append')
    remove_fcn = _fcn_v1('remove')

    def _fcn_v2(name):
        def test(x):
            return x.__getattribute__(name)(v2)
        test.__name__ = name
        return test
    append_fcn2 = _fcn_v2('append')

    def _fcn_item(name):
        def test(x):
            return x.__getattribute__(name)(item1)
        test.__name__ = name
        return test
    pop_fcn = _fcn_item('pop')

    def _fcn_item_v(name):
        def test(x):
            return x.__getattribute__(name)(item1, v1)
        test.__name__ = name
        return test
    insert_fcn = _fcn_item_v('insert')

    def _fcn_item_v2(name):
        def test(x):
            return x.__getattribute__(name)(item1, v2)
        test.__name__ = name
        return test
    insert_fcn2 = _fcn_item_v('insert')

    def _fcn_obj(name):
        def test(x):
            return x.__getattribute__(name)(py_v2)
        test.__name__ = name
        return test
    extend_fcn = _fcn_obj('extend')

    def map_fcn(x):
        return list(map(lambda y: y + 1, x))
    map_fcn.__name__ = 'map'

    def concat_fcn(x):
        return x + py_v2
    concat_fcn.__name__ = 'concat'

    def iconcat_fcn(x):
        x += py_v2
        return None
    iconcat_fcn.__name__ = 'iconcat'

    # Add common tests
    tests = (test_iterator_generator(), )

    # Build additional tests
    if py_class in (list, tuple):
        test_fcns = (count_fcn, index_fcn, concat_fcn, any_fcn, min_fcn, max_fcn, map_fcn)
        if py_class is list:
            test_fcns += (iconcat_fcn, extend_fcn, append_fcn, append_fcn2, remove_fcn,
                          insert_fcn, insert_fcn2, pop_fcn, sort_fcn, reverse_fcn)
            #, copy_fcn)
    elif py_class is dict:
        test_fcns = tuple()
    tests += tuple(test_fcn_generator(test_fcn_i) for test_fcn_i in test_fcns)

    return tests


# Contruct tests
for py_class_n in py_class:
    # Add main test
    test_method = test_generator(py_class_n)
    setattr(TestNested, test_method.__name__, test_method)
    # Add extra tests
    test_methods = test_extra_generator(py_class_n)
    for test_methods_n in test_methods:
        setattr(TestNested, test_methods_n.__name__, test_methods_n)

if __name__ == '__main__':
    try:
        unittest.main()
    except Exception as e:
        if os.path.exists(FILE):
            os.remove(FILE)
        raise e
