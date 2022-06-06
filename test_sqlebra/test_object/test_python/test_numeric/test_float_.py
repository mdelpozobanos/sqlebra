from . import Test_Numeric
from sqlebra.object.python import float_


class Test_float_(Test_Numeric):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._steps_list += ('fromhex', 'hex', 'is_integer')
        # Drop non-applicable tests
        cls._steps_list = (tst
                           for tst in cls._steps_list
                           if tst not in ('denominator', 'numerator', 'invert'))
        # Other variables
        cls.SQLebraClass = float_
        cls.PyClass = float
        cls.x = (10.1, 2.0, float('inf'), float('-inf'), float('nan'))

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def fromhex(self):
        # TODO: Should this be checked?
        pass

    def hex(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(x_n.hex(), self.dbo[n].hex())

    def is_integer(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(x_n.is_integer(), self.dbo[n].is_integer())

    def test_steps(self):
        self._run_steps()
