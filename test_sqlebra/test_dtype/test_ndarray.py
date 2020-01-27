import unittest
import os
import numpy as np
from sqlebra.sqlite import SQLiteDB as DB
# from sqlebra.mysql import MySQLDB as DB
from sqlebra.dtype import ndarray_ as SQLndarray
from sqlebra.dtype import int_ as SQLint
from sqlebra.dtype import tuple_ as SQLtuple
from sqlebra import exceptions as ex

if DB.__name__ == 'MySQLDB':
    FILE = 'unittest'
elif DB.__name__ == 'SQLiteDB':
    FILE = 'unittest.sqlebra.db'


class TestInit(unittest.TestCase):

    value = np.array([10, 11, 12])

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()

    def test_1_set(self):
        self.dbfile['A'] = self.value
        self.assertEqual([None, None, None] + self.value.flatten().tolist(),
                         [r[6] for r in self.dbfile.select(where={'id': 0})])

    def test_2_get(self):
        self.assertIsInstance(self.dbfile['A'], SQLndarray)

    @classmethod
    def tearDownClass(cls):
        cls.dbfile.rm()


class TestInitNested(unittest.TestCase):

    value = np.array([[[1, 2], [3, 4], [5, 6]], [[7, 8], [9, 10], [11, 12]],
                      [[13, 14], [15, 16], [17, 18]], [[19, 20], [21, 22], [23, 24]]])

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()

    def test_1_set(self):
        self.dbfile['A'] = self.value
        self.assertEqual([None, None, None] + self.value.flatten().tolist(),
                         [r[6] for r in self.dbfile.select(where={'id': 0})])

    def test_2_get(self):
        self.assertIsInstance(self.dbfile['A'], SQLndarray)

    def test_3_py(self):
        self.assertListEqual(self.value.tolist(), self.dbfile['A'].py.tolist())

    @classmethod
    def tearDownClass(cls):
        cls.dbfile.rm()


class TestSingle(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()
        value = np.array([10, 11, 12])
        cls.dbfile['A'] = value
        cls.x = [value, cls.dbfile['A']]

    def test_01_get_01_item(self):
        self.assertIsInstance(self.x[1][0], SQLint)

    def test_01_get_02_slice(self):
        self.assertIsInstance(self.x[1][[0]], SQLndarray)

    def test_02_py_01(self):
        self.assertEqual(self.x[0].tolist(), self.x[1].py.tolist())

    def test_02_py_02_item(self):
        self.assertEqual(self.x[0][0], self.x[1][0].py)

    def test_03_edit_01(self):
        self.x[0] = np.array([20, 21, 22])
        self.x[1].py = self.x[0]
        self.assertListEqual(self.x[0].tolist(), self.x[1].py.tolist())

    def test_03_edit_02_item(self):
        for xn in self.x:
            xn[1] = 100
        self.assertListEqual(self.x[0].tolist(), self.x[1].py.tolist())

    def test_03_edit_03_slice_01(self):
        for xn in self.x:
            xn[0:2] = 99
        self.assertListEqual(self.x[0].tolist(), self.x[1].py.tolist())

    def test_03_edit_03_slice_02(self):
        for xn in self.x:
            xn[0:2] = [0, 10]
        self.assertListEqual(self.x[0].tolist(), self.x[1].py.tolist())

    def test_03_edit_03_slice_03(self):
        with self.assertRaises(ValueError):
            self.x[1][0] = [99, 10]
        self.assertListEqual(self.x[0].tolist(), self.x[1].py.tolist())

    def test_99_delete(self):
        self.x[1].delete()
        with self.assertRaises(ex.VariableError):
            self.dbfile['A']

    @classmethod
    def tearDownClass(cls):
        cls.dbfile.rm()


class TestNested(unittest.TestCase):

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


class TestStructured: # (unittest.TestCase):

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
