import unittest
import os
from sqlebra.sqlite import SQLiteDB as DB
# from sqlebra.mysql import MySQLDB as DB
from sqlebra.dtype import dict_ as SQLdict
from sqlebra.dtype import int_ as SQLint
from sqlebra import exceptions as ex

if DB.__name__ == 'MySQLDB':
    FILE = 'unittest'
elif DB.__name__ == 'SQLiteDB':
    FILE = 'unittest.sqlebra.db'


class TestInit(unittest.TestCase):

    value = {'a': 0, 'b': 1, 'c': 2}

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()

    def test_1_set(self):
        self.dbfile['A'] = self.value
        self.assertEqual([None] + [v for v in self.value.values()],
                         [r[6] for r in self.dbfile.select(where={'id': 0})])

    def test_2_get(self):
        self.assertIsInstance(self.dbfile['A'], SQLdict)

    @classmethod
    def tearDownClass(cls):
        cls.dbfile.rm()


class TestNestedInit(unittest.TestCase):

    value = {'a': {'a1': 0, 'a2': 1, 'a3': 2},
             'b': {'b1': 4, 'b2': 5},
             'c': 6}

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()

    def test_1_set(self):
        self.dbfile['A'] = self.value
        self.assertEqual(['dict', 'dict', 'dict', 'int'], [r[2] for r in self.dbfile.select(where={'id': 0})])
        self.assertEqual([None, None, None, 6], [r[6] for r in self.dbfile.select(where={'id': 0})])
        self.assertEqual([None] + [v for v in self.value['a'].values()],
                         [r[6] for r in self.dbfile.select(where={'id': 1})])
        self.assertEqual([None] + [v for v in self.value['b'].values()],
                         [r[6] for r in self.dbfile.select(where={'id': 2})])

    def test_2_get(self):
        self.assertIsInstance(self.dbfile['A'], SQLdict)

    def test_3_py(self):
        self.assertEqual(self.value, self.dbfile['A'].py)

    @classmethod
    def tearDownClass(cls):
        cls.dbfile.rm()


class TestSingle(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()
        value = {'a': 0, 'b': 1, 'c': 2}
        cls.dbfile['A'] = value
        cls.x = [value, cls.dbfile['A']]

    def test_01_get_item(self):
        self.assertIsInstance(self.x[1]['a'], SQLint)

    def test_02_py(self):
        self.assertEqual(self.x[0], self.x[1].py)

    def test_02_1_py_item(self):
        self.assertEqual(self.x[0]['a'], self.x[1]['a'].py)

    def test_03_edit(self):
        self.x[0] = {'A': 20, 'B': 21, 'C': 22}
        self.x[1].py = self.x[0]
        self.assertEqual(self.x[0], self.x[1].py)

    def test_04_1_edit_item(self):
        for xn in self.x:
            xn['A'] = 100
        self.assertEqual(self.x[0], self.x[1].py)

    def test_05_keys(self):
        for key0, key1 in zip(self.x[0].keys(), self.x[1].keys()):
            self.assertEqual(key0, key1)

    def test_06_values(self):
        for value0, value1 in zip(self.x[0].values(), self.x[1].values()):
            self.assertEqual(value0, value1.py)

    def test_07_items(self):
        for item0, item1 in zip(self.x[0].items(), self.x[1].items()):
            self.assertEqual(item0, (item1[0], item1[1].py))

    def test_08_pop(self):
        for x in self.x:
            x.pop('A')
        self.assertEqual(self.x[0], self.x[1].py)

    def test_09_len(self):
        self.assertEqual(len(self.x[0]), len(self.x[1].py))

    def test_10_list(self):
        self.assertEqual(list(self.x[0]), list(self.x[1]))

    @classmethod
    def tearDownClass(cls):
        cls.dbfile.rm()


class TestNested(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()
        value = {'a': {'a1': 0, 'a2': 1, 'a3': 2},
                 'b': {'b1': 4, 'b2': 5},
                 'c': 6}
        cls.dbfile['A'] = value
        cls.x = [value, cls.dbfile['A']]

    def test_01_get_item(self):
        self.assertIsInstance(self.x[1]['a'], SQLdict)
        self.assertIsInstance(self.x[1]['c'], SQLint)

    def test_02_py(self):
        self.assertEqual(self.x[0], self.x[1].py)

    def test_02_1_py_item(self):
        self.assertEqual(self.x[0]['a'], self.x[1]['a'].py)

    def test_03_edit(self):
        self.x[0] = {'A': {'A1': 20, 'A2': 21, 'A3': 22},
                     'B': {'B1': 24, 'B2': 25},
                     'C': 26}
        self.x[1].py = self.x[0]
        self.assertEqual(self.x[0], self.x[1].py)

    def test_04_1_edit_item(self):
        for xn in self.x:
            xn['A'] = 100
        self.assertEqual(self.x[0], self.x[1].py)

    @classmethod
    def tearDownClass(cls):
        cls.dbfile.rm()


class TestEmpty(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dbfile = DB(FILE, mode='w').open()

    def test_1_set(self):
        self.dbfile['A'] = {}

    def test_2_get(self):
        self.assertIsInstance(self.dbfile['A'], SQLdict)

    def test_3_py(self):
        self.assertEqual({}, self.dbfile['A'].py)

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
