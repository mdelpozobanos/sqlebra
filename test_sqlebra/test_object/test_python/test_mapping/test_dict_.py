from . import Test_Maping
from sqlebra.object.python import dict_


class Test_list_(Test_Maping):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._steps_list += ('setitem', 'delitem', 'pop', 'keys', 'values', 'items')
        # Other variables
        cls.SQLebraClass = dict_
        cls.PyClass = dict
        cls.x = ({'a': 0, 'b': '1', 'c': 2.3, 'd': True, 'e': None, 'f': 0, 'g': 10, 'h': 1.1, 'i': 9, 'j': 10},
                 {'a': 0, 'b': '1', 'c': 22.3, 'd': False, 'e': None, '2f': 0, '2g': 10, '2h': 1.1, '2i': 9, '2j': 10})
        cls.x_none = {'a': 0, 'b': 0.0, 'c': False, 'd': '', 'e': None}
        cls.item = 'c'

    def str(self):
        # TODO: From python 3.7 dicts are sorted by order of insertion. This is not supported by SQLebra yet. Test `str`
        #   is overloaded to account for this
        for n, x_n in enumerate(self.x):
            sorted_x_n = {key: value for key, value in sorted(x_n.items())}
            self.assertEqual(str(sorted_x_n), str(self.dbo[n]))

    def iterator(self):
        # TODO: From python 3.7 dicts are sorted by order of insertion. This is not supported by SQLebra yet. Test
        #   `iterator` is overloaded to account for this
        for n, x_n in enumerate(self.x):
            y1 = []
            y2 = []
            for x1, x2 in zip(x_n, self.dbo[n]):
                y1.append(x1)
                y2.append(x2)
            self.assertEqual(sorted(y1), sorted(y2))

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def test_steps(self):
        self._run_steps()

    def setitem(self):
        # Set item
        with self.subTest(mode='Existing item'):
            self.xr['b'] = 'setitem'
            self.dbo['xr']['b'] = 'setitem'
            self.assertEqual(self.xr, self.dbo['xr'])
        with self.subTest(mode='New item'):
            self.xr['new'] = 'item'
            self.dbo['xr']['new'] = 'item'
            self.assertEqual(self.xr, self.dbo['xr'])

    def delitem(self):
        del self.xr['b']
        del self.dbo['xr']['b']
        self.assertEqual(self.xr, self.dbo['xr'])

    def pop(self):
        x1 = self.xr.pop('c')
        x2 = self.dbo['xr'].pop('c')
        self.assertEqual(x1, x2)
        self.assertEqual(self.xr, self.dbo['xr'])

    def keys(self):
        for k1, k2 in zip(self.xr.keys(), self.dbo['xr'].keys()):
            self.assertEqual(k1, k2)

    def values(self):
        for v1, v2 in zip(self.xr.values(), self.dbo['xr'].values()):
            self.assertEqual(v1, v2)

    def items(self):
        for i1, i2 in zip(self.xr.items(), self.dbo['xr'].items()):
            self.assertEqual(i1[0], i2[0])
            self.assertEqual(i1[1], i2[1])
