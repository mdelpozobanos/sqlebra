from test_sqlebra.test_object.test_single import Test_Single
from sqlebra.object.python import str_


class Test_str_(Test_Single):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._steps_list += ('indexing', 'slicing', 'iteration')
        # Other variables
        cls.SQLebraClass = str_
        cls.PyClass = str
        cls.x = ('hello', 'world', 'True')

    def int(self):
        # Assign an integer string value
        self.xr = '123456'
        self.dbo['xr'] = '123456'
        # Cast to integer
        self.assertEqual(int(self.xr), int(self.dbo['xr']))

    def float(self):
        # Assign an integer string value
        self.xr = '12.3456'
        self.dbo['xr'] = '12.3456'
        self.assertEqual(float(self.xr), float(self.dbo['xr']))

    def indexing(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(x_n[n], self.dbo[n][n])

    def slicing(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(x_n[n:n+2], self.dbo[n][n:n+2])

    def iteration(self):
        for x, y in zip(self.xr, self.dbo['xr']):
            self.assertEqual(x, y)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def test_steps(self):
        self._run_steps()
