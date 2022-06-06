from test_sqlebra.test_object.test_multi import Test_Multi

class Test_Sequence(Test_Multi):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._steps_list += ('getitem_slice', 'index', 'count',
                            'any', 'min', 'max', 'map', 'sorted', 'sum')
        cls.item = 1
        cls.slice = slice(1, 7, 2)
        cls.x_none = None  # to be defined
        cls.x_numeric = None  # to be defined

    def init(self):
        super().init()
        self.dbo['x_none'] = self.x_none
        self.dbo['x_numeric'] = self.x_numeric

    def getitem_slice(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(x_n[self.slice], self.dbo[n][self.slice])

    def index(self):
        for n, x_n in enumerate(self.x):
            val = self.dbo[n][n]
            with self.subTest(mode='index_of_py'):
                self.assertEqual(n, self.dbo[n].index(val.py))
            with self.subTest(mode='index_of_objs'):
                self.assertEqual(n, self.dbo[n].index(val))
            with self.assertRaises(ValueError):
                self.dbo[n].index('Value does not exist')

    def count(self):
        for n, x_n in enumerate(self.x):
            with self.subTest(mode='count_of_py'):
                self.assertEqual(x_n.count(0), self.dbo[n].count(0))
                self.assertEqual(x_n.count(1), self.dbo[n].count(1))
                self.assertEqual(x_n.count(False), self.dbo[n].count(False))
                self.assertEqual(x_n.count(True), self.dbo[n].count(True))
            with self.subTest(mode='count_of_objs'):
                self.assertEqual(x_n.count(self.dbo[n][0].py), self.dbo[n].count(self.dbo[n][0]))

    def any(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(any(x_n), any(self.dbo[n]))
        # Test an all false list
        self.assertEqual(any(self.x_none), any(self.dbo['x_none']))

    def min(self):
        self.assertEqual(min(self.x_numeric), min(self.dbo['x_numeric']))

    def max(self):
        self.assertEqual(max(self.x_numeric), max(self.dbo['x_numeric']))

    def map(self):
        self.assertEqual(tuple(map(lambda x: x*100, self.x_numeric)), tuple(map(lambda x: x*100, self.dbo['x_numeric'])))

    def sorted(self):
        self.assertEqual(sorted(self.x_numeric), sorted(self.dbo['x_numeric']))

    def sum(self):
        self.assertEqual(sum(self.x_numeric), sum(self.dbo['x_numeric']))
