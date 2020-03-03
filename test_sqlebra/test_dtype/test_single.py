import os
import builtins
import operator
import unittest

from sqlebra.sqlite import SQLiteDB as DB
# from sqlebra.mysql import MySQLDB as DB
from sqlebra import exceptions as ex
from sqlebra.python import bool_ as SQLbool
from sqlebra.python import int_ as SQLint
from sqlebra.python import float_ as SQLfloat
from sqlebra.python import str_ as SQLstr

# Define
py_class = (builtins.bool, builtins.int, builtins.float, builtins.str)
MAIN_TEST = {}
for py_c in py_class:
    MAIN_TEST[py_c] = True

if DB.__name__ == 'MySQLDB':
    FILE = 'unittest'
elif DB.__name__ == 'SQLiteDB':
    FILE = 'unittest.sqlebra.db'


class TestSingle(unittest.TestCase):
    pass


def test_main_generator(py_class):

    # Set class dependant variables
    if py_class is bool:
        sql_class = SQLbool
        py_v1 = True
        py_v2 = False
        value_col = 2
    elif py_class is int:
        sql_class = SQLint
        py_v1 = 2
        py_v2 = 4
        value_col = 3
    elif py_class is float:
        sql_class = SQLfloat
        py_v1 = 2.0
        py_v2 = 4.0
        value_col = 4
    elif py_class is str:
        sql_class = (SQLbool, SQLint, SQLfloat, SQLstr)
        py_v1 = 'a'
        py_v2 = 'b'
        value_col = 5

    def test(self):

        # Set up
        dbfile = DB(FILE, mode='w').open()

        try:

            obj_row = [0, py_class.__name__, None, None, None, None]

            # test set
            dbfile['A'] = py_v1
            self.assertEqual([('A', 0)],
                             dbfile.select(dbfile.tab_vars, where={'name': 'A'}),
                             msg='Variables table not properly set. Test aborted')
            obj_row[value_col] = py_v1
            self.assertEqual([tuple(obj_row)],
                             dbfile.select(dbfile.tab_objs, where={'id': 0}),
                             msg='Objects table not properly set. Test aborted')

            # test get
            A = dbfile['A']
            self.assertIsInstance(A, sql_class,
                                  msg='Wrong sql_ebra variable type retrieved. Test aborted')

            # test py
            Apy = A.py
            self.assertIsInstance(Apy, py_class,
                                  msg='Wrong variable type retrieved. Test aborted')
            self.assertEqual(py_v1, Apy,
                             msg='Wrong object value retrieved. Test aborted')

            # Test set py
            id = A.id
            A.py = py_v2
            self.assertEqual(id, A.id,
                             msg='sql_-object id not recycled. Test aborted')
            self.assertEqual(py_v2, A.py,
                             msg='sql_-object value not updated. Test aborted')

            # Test delete
            A.delete()
            with self.assertRaises(ex.VariableError,
                                   msg='sql_-variable not deleted. Test aborted'):
                dbfile['A']
            with self.assertRaises(ex.ObjectError,
                                   msg='sql_-object not deleted. Test aborted'):
                dbfile[0]

        except:
            MAIN_TEST[py_class] = False
            raise
        finally:  # Be clean
            dbfile.rm()

    # Adjust name
    test.__name__ = '{}_{}_00_main'.format(test.__name__, py_class.__name__)

    return test


def test_numeric_generator(py_class):

    # Set class dependant variables
    if py_class is bool:
        py_v1 = True
        py_v2 = False
        value_ind = 2
    elif py_class is int:
        py_v1 = 2
        py_v2 = 4
        value_ind = 3
    elif py_class is float:
        py_v1 = 2.0
        py_v2 = 4.0
        value_ind = 4
    elif py_class is str:
        py_v1 = 'hello'
        py_v2 = 'world'
        value_ind = 5

    def test_operators_generator(fcn):

        def test(self):

            self.assertTrue(MAIN_TEST[py_class],
                            msg='Main test not past. Ignore specific tests')

            # Set up
            dbfile = DB(FILE, mode='w').open()
            dbfile['x'] = py_v1
            x = dbfile['x']
            dbfile['x2'] = py_v2
            x2 = dbfile['x2']

            try:
                # Apply function
                self.assertEqual(fcn(py_v1, py_v2), fcn(x, py_v2))
                # Apply function
                self.assertEqual(fcn(py_v1, py_v2), fcn(x, x2))
            finally:  # Be clean
                dbfile.rm()

        # Adjust name
        test.__name__ = '{}_{}'.format(test.__name__, fcn.__name__)
        return test

    def test_inplace_operators_generator(fcn):

        def test(self):

            self.assertTrue(MAIN_TEST[py_class],
                            msg='Main test not past. Ignore specific tests')

            # Set up
            dbfile = DB(FILE, mode='w').open()
            dbfile['x'] = py_v1
            x = dbfile['x']
            dbfile['x2'] = py_v2
            x2 = dbfile['x2']
            try:
                # Apply function
                v1 = fcn(py_v1, py_v2)
                x = fcn(x, py_v2)
                self.assertEqual(v1, x.py)
                self.assertEqual(2, dbfile.select(dbfile.tab_objs, column=('count(*)', ))[0][0])
                # Reset value
                dbfile['x'] = py_v1
                x = dbfile['x']
                # Apply function
                x = fcn(x, x2)
                self.assertEqual(v1, x.py)
                self.assertEqual(2, dbfile.select(dbfile.tab_objs, column=('count(*)', ))[0][0])
            finally:  # Be clean
                dbfile.rm()

        # Adjust name
        test.__name__ = '{}_{}'.format(test.__name__, fcn.__name__)
        return test

    def test_unary_operators_generator(fcn):

        def test(self):

            self.assertTrue(MAIN_TEST[py_class],
                            msg='Main test not past. Ignore specific tests')

            # Set up
            dbfile = DB(FILE, mode='w').open()
            dbfile['x'] = py_v1
            x = dbfile['x']

            try:
                # Apply function
                self.assertEqual(fcn(py_v1), fcn(x))
                # Apply function
                self.assertEqual(fcn(py_v1), fcn(x))
            finally:  # Be clean
                dbfile.rm()

        # Adjust name
        test.__name__ = '{}_{}'.format(test.__name__, fcn.__name__)
        return test

    def test_str_indexing(self):

        self.assertTrue(MAIN_TEST[py_class],
                        msg='Main test not past. Ignore specific tests')

        # Set up
        dbfile = DB(FILE, mode='w').open()
        dbfile['x'] = py_v1
        x = dbfile['x']

        try:
            # Apply function
            self.assertEqual(py_v1[0], x[0])
            # Apply function
            self.assertEqual(py_v1[-1], x[-1])
        finally:  # Be clean
            dbfile.rm()

    def test_str_iterator(self):

        self.assertTrue(MAIN_TEST[py_class],
                        msg='Main test not past. Ignore specific tests')

        # Set up
        dbfile = DB(FILE, mode='w').open()
        dbfile['x'] = py_v1
        x = dbfile['x']

        try:
            # Apply function
            self.assertEqual(py_v1[0], x[0])
            # Apply function
            self.assertEqual(py_v1[-1], x[-1])
        finally:  # Be clean
            dbfile.rm()

    if py_class is bool:
        operators = (operator.add, operator.sub)
        test_methods = [test_operators_generator(fcn) for fcn in operators]
        # Add unary operators
        operators = (operator.neg, operator.pos, operator.invert)
        test_methods += [test_unary_operators_generator(fcn) for fcn in operators]
    else:
        operators = (operator.add, operator.sub, operator.mul, operator.truediv, operator.floordiv, operator.mod,
                     operator.pow, operator.lt, operator.gt, operator.le, operator.ge, operator.eq, operator.ne)
        test_methods = [test_operators_generator(fcn) for fcn in operators]
        # Add in place operators
        operators = (operator.iadd, operator.isub, operator.imul, operator.itruediv, operator.ifloordiv,
                     operator.imod, operator.ipow)
        test_methods += [test_inplace_operators_generator(fcn) for fcn in operators]

    if py_class is str:
        operators = (len, )
        test_methods += [test_unary_operators_generator(fcn) for fcn in operators]
        test_methods += [test_str_indexing, test_str_iterator]

    # Adjust names
    for n in range(len(test_methods)):
        aux = test_methods[n].__name__.split('_')
        test_methods[n].__name__ = '{}_{}_{}'.format(aux[0], py_class.__name__, aux[1])

    return test_methods


def test_extra_generator(py_class):

    # Set class dependant variables
    if py_class is bool:
        py_v1 = True
        py_v2 = False
        value_ind = 2
    elif py_class is int:
        py_v1 = 2
        py_v2 = 4
        value_ind = 3
    elif py_class is float:
        py_v1 = 2.0
        py_v2 = 4.0
        value_ind = 4
    elif py_class is str:
        py_v1 = 'hello'
        py_v2 = 'world'
        value_ind = 5

    def test_0arg_generator(fcn):

        def test(self):

            self.assertTrue(MAIN_TEST[py_class],
                            msg='Main test not past. Ignore specific tests')

            # Set up
            dbfile = DB(FILE, mode='w').open()
            dbfile['x'] = py_v1
            x = dbfile['x']

            try:
                # Apply function
                self.assertEqual(fcn(py_v1), fcn(x))
                # Apply function
                self.assertEqual(fcn(py_v1), fcn(x))
            finally:  # Be clean
                dbfile.rm()

        # Adjust name
        test.__name__ = '{}_{}'.format(test.__name__, fcn.__name__)
        return test

    def test_indexing(self):

        self.assertTrue(MAIN_TEST[py_class],
                        msg='Main test not past. Ignore specific tests')

        # Set up
        dbfile = DB(FILE, mode='w').open()
        dbfile['x'] = py_v1
        x = dbfile['x']

        try:
            # Apply function
            self.assertEqual(py_v1[0], x[0])
            # Apply function
            self.assertEqual(py_v1[-1], x[-1])
        finally:  # Be clean
            dbfile.rm()

    def test_iterator(self):

        self.assertTrue(MAIN_TEST[py_class],
                        msg='Main test not past. Ignore specific tests')

        # Set up
        dbfile = DB(FILE, mode='w').open()
        dbfile['x1'] = py_v1
        x1 = dbfile['x1']

        try:
            for v, x in zip(py_v1, x1):
                self.assertEqual(v, x)
        finally:  # Be clean
            dbfile.rm()

    if py_class is str:
        operators = (len, )
        test_methods = [test_0arg_generator(fcn) for fcn in operators]
        test_methods += [test_indexing, test_iterator]
    else:
        test_methods = []

    # Adjust names
    for n in range(len(test_methods)):
        aux = test_methods[n].__name__.split('_')
        test_methods[n].__name__ = '{}_{}_{}'.format(aux[0], py_class.__name__, aux[1])

    return test_methods


for py_class_n in py_class:
    test_method = test_main_generator(py_class_n)
    setattr(TestSingle, test_method.__name__, test_method)
    # Add extra tests
    test_methods = test_extra_generator(py_class_n)
    for test_methods_n in test_methods:
        setattr(TestSingle, test_methods_n.__name__, test_methods_n)
    # Add numeric tests
    if py_class_n is not str:
        test_methods = test_numeric_generator(py_class_n)
        for test_methods_n in test_methods:
            setattr(TestSingle, test_methods_n.__name__, test_methods_n)

if __name__ == '__main__':
    try:
        unittest.main()
    except Exception as e:
        if os.path.exists(FILE):
            os.remove(FILE)
        raise e
