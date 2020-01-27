import unittest
import os
from sqlebra.sqlite import SQLiteDB as DB
from sqlebra.dtype import list_ as SQLlist
from sqlebra.dtype import int_ as SQLint
from sqlebra import exceptions as ex

FILE = 'unittest.sqlebra.db'


class TestInit(unittest.TestCase):

    value = [10, 11, 12]

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()

    def test_1_set(self):
        self.dbfile['A'] = self.value
        self.assertEqual([None] + self.value, [r[6] for r in self.dbfile.select(where={'id': 0})])

    def test_2_get(self):
        self.assertIsInstance(self.dbfile['A'], SQLlist)

    @classmethod
    def tearDownClass(cls):
        cls.dbfile.disconnect()
        os.remove(FILE)


class TestInitNested(unittest.TestCase):

    value = [[10, 11, 12], [13, 14], 15]

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()

    def test_1_set(self):
        self.dbfile['A'] = self.value
        self.assertEqual(['list', 'list', 'list', 'int'], [r[2] for r in self.dbfile.select(where={'id': 0})])
        self.assertEqual([None, None, None, 15], [r[6] for r in self.dbfile.select(where={'id': 0})])
        self.assertEqual([None] + self.value[0], [r[6] for r in self.dbfile.select(where={'id': 1})])
        self.assertEqual([None] + self.value[1], [r[6] for r in self.dbfile.select(where={'id': 2})])

    def test_2_get(self):
        self.assertIsInstance(self.dbfile['A'], SQLlist)

    def test_3_py(self):
        self.dbfile[0]
        self.assertEqual(self.value, self.dbfile['A'].py)

    @classmethod
    def tearDownClass(cls):
        cls.dbfile.disconnect()
        os.remove(FILE)


class TestSingle(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()
        value = [10, 11, 12]
        cls.dbfile['A'] = value
        cls.x = [value, cls.dbfile['A']]

    def test_01_get_item(self):
        self.assertIsInstance(self.x[1][0], SQLint)

    def test_02_py(self):
        self.assertEqual(self.x[0], self.x[1].py)

    def test_02_1_py_item(self):
        self.assertEqual(self.x[0][0], self.x[1][0].py)

    def test_03_edit(self):
        self.x[0] = [20, 21, 22]
        self.x[1].py = [20, 21, 22]
        self.assertEqual(self.x[0], self.x[1].py)

    def test_04_1_edit_item(self):
        for xn in self.x:
            xn[1] = 100
        self.assertEqual(self.x[0], self.x[1].py)

    def test_05_append(self):
        for xn in self.x:
            xn.append(5)
        self.assertEqual(self.x[0], self.x[1].py)

    def test_06_extend(self):
        for xn in self.x:
            xn.extend([6, 3, 0])
        self.assertEqual(self.x[0], self.x[1].py)

    def test_07_insert(self):
        for xn in self.x:
            xn.insert(2, 7)
        self.assertEqual(self.x[0], self.x[1].py)

    def test_08_index(self):
        self.assertEqual(self.x[0].index(100), self.x[1].index(100))

    def test_09_remove(self):
        self.assertEqual(self.x[0].remove(100), self.x[1].remove(100))

    def test_10_pop(self):
        self.assertEqual(self.x[0].pop(), self.x[1].pop())
        self.assertEqual(self.x[0], self.x[1].py)

    def test_11_count(self):
        self.assertEqual(self.x[0].count(5), self.x[1].count(5))

    def test_12_iter(self):
        for x0, x1 in zip(self.x[0], self.x[1]):
            self.assertEqual(x0, x1.py)

    def test_99_delete(self):
        self.x[1].delete()
        with self.assertRaises(ex.VariableError):
            self.dbfile['A']

    @classmethod
    def tearDownClass(cls):
        cls.dbfile.disconnect()
        os.remove(FILE)


class TestNested(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()
        value = [[10, 11, 12], [13, 14], 15]
        cls.dbfile['A'] = value
        cls.x = [value, cls.dbfile['A']]

    def test_01_get_item(self):
        self.assertIsInstance(self.x[1][0], SQLlist)
        self.assertIsInstance(self.x[1][2], SQLint)

    def test_02_py(self):
        self.assertEqual(self.x[0], self.x[1].py)

    def test_02_1_py_item(self):
        self.assertEqual(self.x[0][0], self.x[1][0].py)

    def test_03_edit(self):
        self.x[0] = [[20, 21, 22], [23, 24], 25]
        self.x[1].py = self.x[0]
        self.assertEqual(self.x[0], self.x[1].py)

    def test_03_1_edit_item(self):
        for xn in self.x:
            xn[0] = 100
        self.assertEqual(self.x[0], self.x[1].py)

    @classmethod
    def tearDownClass(cls):
        cls.dbfile.disconnect()
        os.remove(FILE)


class TestEmpty(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()

    def test_1_set(self):
        self.dbfile['A'] = []

    def test_2_get(self):
        self.assertIsInstance(self.dbfile['A'], SQLlist)

    def test_3_py(self):
        self.assertEqual([], self.dbfile['A'].py)

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
