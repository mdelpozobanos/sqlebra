from . import Test_Sequence
from sqlebra.object.python import list_


class Test_list_(Test_Sequence):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._steps_list += ('setitem', 'delitem', 'append', 'extend', 'insert', 'remove', 'pop', 'reverse')
        # Other variables
        cls.SQLebraClass = list_
        cls.PyClass = list
        cls.x = ([0, '1', 2.3, True, None, 0, 10, 1.1, 9, 10],
                 [0, '11', 12.3, None, False, 0, 10, 1.1, 9, 10],
                 [0, 1, [0, 1, [0, 1]]])
        cls.x_none = [0, 0.0, False, '', None]
        cls.x_numeric = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def test_steps(self):
        self._run_steps()

    def setitem(self):
        # Set item
        with self.subTest(setitem_with='python value'):
            self.xr[3] = 'setitem'
            self.dbo['xr'][3] = 'setitem'
            self.assertEqual(self.xr, self.dbo['xr'])
        with self.subTest(setitem_with='sqlebra object'):
            self.xr[3] = self.xr[2]
            self.dbo['xr'][3] = self.dbo['xr'][2]
            self.assertEqual(self.xr, self.dbo['xr'])
        with self.subTest(setitem_with='itself'):
            self.xr[3] = self.xr[3]
            self.dbo['xr'][3] = self.dbo['xr'][3]
            self.assertEqual(self.xr, self.dbo['xr'])

    def delitem(self):
        with self.subTest(mode='Single'):
            del self.xr[3]
            del self.dbo['xr'][3]
            self.assertEqual(self.xr, self.dbo['xr'])
        with self.subTest(mode='slice'):
            del self.xr[:2]
            del self.dbo['xr'][:2]
            self.assertEqual(self.xr, self.dbo['xr'])

    def append(self):
        self.xr.append(0)
        self.dbo['xr'].append(0)
        self.assertEqual(self.xr, self.dbo['xr'])

    def extend(self):
        self.xr.extend([1, 2, 3])
        self.dbo['xr'].extend([1, 2, 3])
        self.assertEqual(self.xr, self.dbo['xr'])

    def insert(self):
        self.xr.insert(3, -1)
        self.dbo['xr'].insert(3, -1)
        self.assertEqual(self.xr, self.dbo['xr'])

    def remove(self):
        self.xr.remove(2)
        self.dbo['xr'].remove(2)
        self.assertEqual(self.xr, self.dbo['xr'])

    def pop(self):
        x1 = self.xr.pop(2)
        x2 = self.dbo['xr'].pop(2)
        self.assertEqual(x1, x2)
        self.assertEqual(self.xr, self.dbo['xr'])

    def reverse(self):
        self.xr.reverse()
        self.dbo['xr'].reverse()
        self.assertEqual(self.xr, self.dbo['xr'])