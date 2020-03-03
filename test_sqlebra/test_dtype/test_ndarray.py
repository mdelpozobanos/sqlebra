import unittest
import os
import numpy as np
from sqlebra.sqlite import SQLiteDB as DB
# from sqlebra.mysql import MySQLDB as DB
from sqlebra.numpy import ndarray_ as SQLndarray
from sqlebra.object.single import Single as SQLSingle
from sqlebra import exceptions as ex

py_class = np.ndarray
sql_class = SQLndarray

if DB.__name__ == 'MySQLDB':
    FILE = 'unittest'
elif DB.__name__ == 'SQLiteDB':
    FILE = 'unittest.sqlebra.db'


class TestSQLndarray(unittest.TestCase):

    def check_db(self, dbfile, num_vars, num_objs, num_items, msg):
        self.assertEqual(num_vars, dbfile.select(dbfile.tab_vars, column=('count(*)',))[0][0],
                         msg=msg.format('Bad variables table'))
        self.assertEqual(num_objs, dbfile.select(dbfile.tab_objs, column=('count(*)',))[0][0],
                         msg=msg.format('Bad objects table'))
        self.assertEqual(num_items, dbfile.select(dbfile.tab_items, column=('count(*)',))[0][0],
                         msg=msg.format('Bad items table'))


def test_main_generator():
    def test_generator(name):

        # Set variables
        if name == 'vector':
            py_v0 = np.array([])
            py_v1 = np.arange(10, 30)
            py_v2 = np.random.randn(10)
            item1 = 0
            item2 = -1
            slices = [(slice(0, None, 3)),
                      ([0, 0, 1]),
                      (None, slice(5, -5, 2))]
        elif name == 'matrix':
            py_v0 = np.array([])
            py_v1 = np.random.rand(2, 3, 6)
            py_v2 = np.random.rand(4, 3, 5)
            item1 = (0, 0, 1)
            item2 = (-1, -1, 1)
            slices = [(1, slice(0, 1)),
                      (0, 0, slice(1, -1, 2)),
                      (..., slice(1, -1, 2)),
                      (1, ..., slice(1, -1, 2)),
                      (0, 0, slice(1, -1, 2), None)]

        def test(self):

            # Set up
            dbfile = DB(FILE, mode='w').open()

            try:

                # Basic
                # ---------------------------------------------------

                for py_v in (py_v0, py_v1, py_v2):

                    # Instantiate
                    msg = '[value {}] INSTANTIATE: {{}}. TEST ABORTED'.format(py_v)
                    dbfile['x'] = py_v
                    aux = (2 + py_v.size) + len(py_v.dtype.descr) + sum([len(i) for i in py_v.dtype.descr]) + \
                          len(py_v.shape)
                    self.check_db(dbfile, 1, 1 + aux, aux, msg)

                    # Retrieve
                    msg = '[value {}] RETRIEVE: {{}}. TEST ABORTED'.format(py_v)
                    x = dbfile['x']
                    self.assertIsInstance(x, sql_class, msg=msg.format('Bad SQLebra class'))

                    # py
                    msg = '[value {}] PY: {{}}. TEST ABORTED'.format(py_v)
                    xpy = x.py
                    self.assertIsInstance(xpy, py_class, msg=msg.format('Bad python class'))
                    self.assertTrue((py_v == xpy).all(), msg=msg.format('Bad array'))

                    # Simple indexing -----------------------------------------------------------------
                    msg = '[value {} - item {}] INDEXING: {}. TEST ABORTED'

                    for item in (item1, item2):
                        if py_v.size == 0:
                            with self.assertRaises(IndexError):
                                x[item]
                        else:
                            pyitem = py_v[item]
                            xitem = x[item]
                            if isinstance(xitem, SQLSingle):
                                self.assertEqual(1, pyitem.size, msg=msg.format(py_v, item, 'Bad size'))
                                self.assertEqual(tuple(), pyitem.shape, msg=msg.format(py_v, item, 'Bad shape'))
                                self.assertEqual(type(pyitem.item()), xitem.pyclass,
                                                 msg=msg.format(py_v, item, 'Bad python class'))
                            else:
                                self.assertEqual(pyitem.size, xitem.size, msg=msg.format(py_v, item, 'Bad size'))
                                self.assertEqual(pyitem.shape, xitem.shape, msg=msg.format(py_v, item, 'Bad shape'))
                                self.assertEqual(pyitem.dtype, xitem.dtype, msg=msg.format(py_v, item, 'Bad dtype'))
                            self.assertTrue((pyitem == xitem.py).all(), msg=msg.format(py_v, item, 'Bad array'))

                    # Clearing  -----------------------------------------------------------------
                    msg = '[value {}] CLEAR {{}}. TEST ABORTED'.format(py_v)

                    # Test clear
                    x.clear()
                    self.check_db(dbfile, 1, 1, 0, msg)
                    self.assertEqual(py_v0.size, x.size, msg=msg.format(py_v, 'Bad size'))
                    self.assertEqual(py_v0.shape, x.shape, msg=msg.format(py_v, 'Bad shape'))
                    self.assertTrue((py_v0 == x.py).all(), msg=msg.format(py_v, 'Bad array'))

                    # Setting py -----------------------------------------------------------------
                    msg = '[value {}] SETTING PY {{}}. TEST ABORTED'.format(py_v)

                    id = x.id
                    x.py = py_v
                    aux = (2 + py_v.size) + len(py_v.dtype.descr) + sum([len(i) for i in py_v.dtype.descr]) + \
                          len(py_v.shape)
                    self.check_db(dbfile, 1, 1 + aux, aux, msg)
                    self.assertEqual(id, x.id, msg=msg.format('SQL-object id not recycled'))
                    self.assertTrue((py_v == x.py).all(), msg=msg.format('Bad array'))

                    # Over-writing py -----------------------------------------------------------------
                    msg = '[value {}] OVER-WRITING PY {{}}. TEST ABORTED'.format(py_v)

                    id = x.id
                    x.py = py_v
                    aux = (2 + py_v.size) + len(py_v.dtype.descr) + sum([len(i) for i in py_v.dtype.descr]) + \
                          len(py_v.shape)
                    self.check_db(dbfile, 1, 1 + aux, aux, msg)
                    self.assertEqual(id, x.id, msg=msg.format('SQL-object id not recycled'))
                    self.assertTrue((py_v == x.py).all(), msg=msg.format('Bad array'))

                    # Over-writing variable -----------------------------------------------------------------
                    msg = '[value {}] OVER-WRITING VARIABLE: {{}}. TEST ABORTED'.format(py_v)

                    dbfile['x'] = py_v
                    xpy = dbfile['x'].py
                    aux = (2 + py_v.size) + len(py_v.dtype.descr) + sum([len(i) for i in py_v.dtype.descr]) + \
                          len(py_v.shape)
                    self.check_db(dbfile, 1, 1 + aux, aux, msg)
                    self.assertTrue((py_v == xpy).all(), msg=msg.format('Bad array'))

                # Editing -----------------------------------------------------------------
                msg = 'ITEM EDIT: {}. TEST ABORTED'

                # Set with python value
                x = dbfile['x']
                x.py = py_v2
                py_v2[item1] = py_v1[item1]
                x[item1] = py_v1[item1]
                self.check_db(dbfile, 1, 1 + aux, aux, msg)
                self.assertEqual(py_v2[item1], x[item1].py, msg=msg.format('Not updated'))

                # Set with SQLebra object
                py_v2[item1] = py_v2[item2]
                x[item1] = x[item2]
                self.check_db(dbfile, 1, 1 + aux - 1, aux, msg)
                self.assertEqual(x[item1].py, x[item2].py, msg=msg.format('Not updated'))

                # Slicing ------------------------------------------------

                for slice_n in slices:
                    msg = '[Slice {}] SLICING: {{}}. TEST ABORTED'.format(slice_n)
                    # test slice
                    py_s = py_v2[slice_n]
                    s = x[slice_n]
                    self.assertEqual(len(py_s), len(s), msg=msg.format('Bad length'))
                    self.assertEqual(py_s.shape, s.shape, msg=msg.format('Bad shape'))
                    self.assertTrue((py_s == s.py).all(), msg=msg.format('Bad py'))
                    # test slice indexing
                    self.assertTrue((py_s[0] == s[0].py).all(), msg=msg.format('Bad item[0]'))
                    self.assertTrue((py_s[-1] == s[-1].py).all(), msg=msg.format('Bad imte[-1]'))
                    self.assertTrue((py_s[1:-1] == s[1:-1].py).all(), msg=msg.format('Bad item[1:-1]'))

                # Deleting ------------------------------------------------
                msg = 'DELETE: {}. TEST ABORTED'

                # Test delete
                x.delete()
                with self.assertRaises(ex.VariableError, msg=msg.format('Not variable error raised')):
                    dbfile['x']
                with self.assertRaises(ex.ObjectError, msg=msg.format('Not object error raised')):
                    dbfile[0]
                self.check_db(dbfile, 0, 0, 0, msg)

            finally:  # Tear down
                dbfile.rm()

        test.__name__ = '{}_{}_main'.format(test.__name__, name)
        return test

    def test_advanced_indexing_generator(name):

        if name == 'vector':
            py_v1 = np.arange(10, 30)
            py_v2 = np.random.randn(10)
            item1 = 0
            item2 = -1
            slices = [([0, 0, 1])]
        elif name == 'matrix':
            py_v1 = np.random.rand(2, 3, 6)
            py_v2 = np.random.rand(4, 3, 5)
            item1 = (0, 0, 1)
            item2 = (-1, -1, 1)
            slices = [([0, 0, 1], slice(0, 1)),
                      (0, 0, slice(1, -1, 2), None),
                      (0, None, [0, 2], slice(1, -1, 2))]

        def test(self):

            # Set up
            dbfile = DB(FILE, mode='w').open()
            dbfile['x'] = py_v1
            x = dbfile['x']

            try:

                # Advance indexing ------------------------------------------------

                for slice_n in slices:
                    # test slice
                    py_s = py_v1[slice_n]
                    s = x[slice_n]
                    self.assertEqual(len(py_s), len(s),
                                     msg='[slice {}] Wrong slice length. Test aborted'.format(slice_n))
                    self.assertEqual(py_s.shape, s.shape,
                                     msg='[slice {}] Wrong shape retrieved. Test aborted'.format(slice_n))
                    self.assertTrue((py_s == s.py).all(),
                                    msg='[slice {}] Wrong slice retrieved. Test aborted'.format(slice_n))
                    # test slice indexing
                    self.assertTrue((py_s[0] == s[0].py).all(),
                                    msg='[slice {}][0] Wrong slice item retrieved. Test aborted'.format(slice_n))
                    self.assertTrue((py_s[-1] == s[-1].py).all(),
                                    msg='[slice {}][-1] Wrong slice negative item retrieved. Test aborted'.format(
                                        slice_n))
                    self.assertTrue((py_s[1:-1] == s[1:-1].py).all(),
                                    msg='[slice {}][1:-1] Wrong slice item retrieved. Test aborted'.format(slice_n))
            finally:  # Tear down
                dbfile.rm()

        test.__name__ = '{}_{}_advance_indexing'.format(test.__name__, name)
        return test

    def test_advanced_setitem_generator(name):

        if name == 'vector':
            py_v1 = np.arange(10, 30)
            items = (slice(0, 4), slice(2, 4))
            values = (10, [2, 3])
        elif name == 'matrix':
            py_v1 = np.random.rand(2, 3, 4)
            items = ((0, slice(0, 3)), slice(0, 1), slice(0, 1))
            values = (10, np.random.rand(1, 3, 4), np.random.rand(1, 3, 4))

        def test(self):

            # Set up
            dbfile = DB(FILE, mode='w').open()
            dbfile['x'] = py_v1
            x = dbfile['x']

            try:
                for item, value in zip(items, values):
                    msg = 'x[{}] = {} || {{}}'.format(item, value)

                    # Setting from python value
                    py_v1[item] = value
                    x[item] = value
                    self.assertTrue((py_v1 == x.py).all(), msg=msg.format('Bad array'))

                    # Reset
                    x[item] = 0

                    # Setting from sql value
                    dbfile['value'] = value
                    x[item] = dbfile['value']
                    self.assertTrue((py_v1 == x.py).all(), msg=msg.format('Bad array'))

            finally:  # Tear down
                dbfile.rm()

        test.__name__ = '{}_{}_advance_setitem'.format(test.__name__, name)
        return test

    # Vector stile
    tests = []
    for name in ('vector', 'matrix'):
        tests.extend([test_generator(name),
                      test_advanced_indexing_generator(name),
                      test_advanced_setitem_generator(name)])

    return tests


# Build test class
for test_method in test_main_generator():
    setattr(TestSQLndarray, test_method.__name__, test_method)

    # def test_03_edit_03_slice_01(self):
    #     for xn in self.x:
    #         xn[0:2] = 99
    #     self.assertListEqual(self.x[0].tolist(), self.x[1].py.tolist())
    #
    # def test_03_edit_03_slice_02(self):
    #     for xn in self.x:
    #         xn[0:2] = [0, 10]
    #     self.assertListEqual(self.x[0].tolist(), self.x[1].py.tolist())
    #
    # def test_03_edit_03_slice_03(self):
    #     with self.assertRaises(ValueError):
    #         self.x[1][0] = [99, 10]
    #     self.assertListEqual(self.x[0].tolist(), self.x[1].py.tolist())


class TestNested:  # (unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()
        value = np.array([[[1, 2], [3, 4], [5, 6]], [[7, 8], [9, 10], [11, 12]],
                          [[13, 14], [15, 16], [17, 18]], [[19, 20], [21, 22], [23, 24]]])
        cls.dbfile['A'] = value
        cls.x = [value, cls.dbfile['A']]
        cls.dbfile.commit()

    def test_01_get_01_item(self):
        self.assertIsInstance(self.x[1][0, 0, 0], SQLint)

    def test_01_get_02_slice(self):
        self.assertIsInstance(self.x[1][0], SQLndarray)

    def test_02_py_01(self):
        self.assertListEqual(self.x[0].tolist(), self.x[1].py.tolist())

    def test_02_py_02_item(self):
        self.assertEqual(self.x[0][0, 0, 0], self.x[1][0, 0, 0].py)

    def test_02_py_03_slice_01(self):
        self.assertListEqual(self.x[0][0].tolist(), self.x[1][0].py.tolist())

    def test_02_py_04_slice_02(self):
        self.assertListEqual(self.x[0][0, [0, 2]].tolist(), self.x[1][0, [0, 2]].py.tolist())

    def test_02_py_04_slice_03(self):
        self.assertListEqual(self.x[0][0, 0:2].tolist(), self.x[1][0, 0:2].py.tolist())

    def test_02_py_04_slice_04(self):
        for n0 in range(self.x[0].shape[0]):
            for n1 in range(self.x[0].shape[1]):
                self.assertListEqual(self.x[0][n0:, :][:, n1:].tolist(), self.x[1][n0:, :][:, n1:].py.tolist())

    def test_03_edit_01(self):
        self.x[0] = np.array([[20, 21, 22], [23, 24, 25]])
        self.x[1].py = self.x[0]
        self.assertListEqual(self.x[0].tolist(), self.x[1].py.tolist())

    def test_03_edit_02_item(self):
        for xn in self.x:
            xn[0, 1] = 100
        self.assertListEqual(self.x[0].tolist(), self.x[1].py.tolist())

    def test_03_edit_03_slice_01(self):
        for xn in self.x:
            xn[0] = 999
        self.assertListEqual(self.x[0].tolist(), self.x[1].py.tolist())

    def test_03_edit_03_slice_02(self):
        for xn in self.x:
            xn[0] = [10, 20, 30]
        self.assertListEqual(self.x[0].tolist(), self.x[1].py.tolist())

    def test_03_edit_03_slice_03(self):
        with self.assertRaises(ValueError):
            self.x[1][0] = [10, 20, 'a']
        self.assertListEqual(self.x[0].tolist(), self.x[1].py.tolist())

    def test_04_len(self):
        self.assertEqual(len(self.x[0]), len(self.x[1]))

    def test_05_add_dim(self):
        self.assertListEqual(self.x[0][:, None].tolist(), self.x[1][:, None].py.tolist())

    @classmethod
    def tearDownClass(cls):
        cls.dbfile.rm()


class aa_TestStructured:  # (unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()
        value = np.array([(1, 1.1, 'a'),
                          (2, 2.2, 'b'),
                          (3, 3.3, 'c'),
                          (4, 4.4, 'd')],
                         dtype=[('col1', int), ('col2', float), ('col3', str)])
        cls.dbfile['A'] = value
        cls.x = [value, cls.dbfile['A']]
        cls.dbfile.commit()

    def test_01_get_01_row(self):
        self.assertIsInstance(self.x[1][0], SQLtuple)

    def test_01_get_02_col(self):
        # self.x[0]['col1']
        self.assertIsInstance(self.x[1]['col1'], SQLndarray)

    def test_01_get_03_value(self):
        self.assertIsInstance(self.x[1][0][0], SQLint)

    def test_01_get_04_slice(self):
        self.assertIsInstance(self.x[1]['col1'], SQLndarray)

    def test_02_py_01(self):
        self.assertListEqual(self.x[0].tolist(), self.x[1].py.tolist())

    def test_02_py_02_item(self):
        self.assertEqual(self.x[0][0, 0, 0], self.x[1][0, 0, 0].py)

    def test_02_py_03_slice_01(self):
        self.assertListEqual(self.x[0][0].tolist(), self.x[1][0].py.tolist())

    def test_02_py_04_slice_02(self):
        self.assertListEqual(self.x[0][0, [0, 2]].tolist(), self.x[1][0, [0, 2]].py.tolist())

    def test_02_py_04_slice_03(self):
        self.assertListEqual(self.x[0][0, 0:2].tolist(), self.x[1][0, 0:2].py.tolist())

    def test_02_py_04_slice_04(self):
        for n0 in range(self.x[0].shape[0]):
            for n1 in range(self.x[0].shape[1]):
                self.assertListEqual(self.x[0][n0:, :][:, n1:].tolist(), self.x[1][n0:, :][:, n1:].py.tolist())

    def test_03_edit_01(self):
        self.x[0] = np.array([[20, 21, 22], [23, 24, 25]])
        self.x[1].py = self.x[0]
        self.assertListEqual(self.x[0].tolist(), self.x[1].py.tolist())

    def test_03_edit_02_item(self):
        for xn in self.x:
            xn[0, 1] = 100
        self.assertListEqual(self.x[0].tolist(), self.x[1].py.tolist())

    def test_03_edit_03_slice_01(self):
        for xn in self.x:
            xn[0] = 999
        self.assertListEqual(self.x[0].tolist(), self.x[1].py.tolist())

    def test_03_edit_03_slice_02(self):
        for xn in self.x:
            xn[0] = [10, 20, 30]
        self.assertListEqual(self.x[0].tolist(), self.x[1].py.tolist())

    def test_03_edit_03_slice_03(self):
        with self.assertRaises(ValueError):
            self.x[1][0] = [10, 20, 'a']
        self.assertListEqual(self.x[0].tolist(), self.x[1].py.tolist())

    def test_04_len(self):
        self.assertEqual(len(self.x[0]), len(self.x[1]))

    def test_05_add_dim(self):
        self.assertListEqual(self.x[0][:, None].tolist(), self.x[1][:, None].py.tolist())

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
