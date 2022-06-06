from .test_numeric._test_numeric import _Test_Numeric
from sqlebra.object.python import bool_


class Test_bool_(_Test_Numeric):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        # Drop non-applicable tests
        cls._steps_list = (tst
                           for tst in cls._steps_list
                           if tst not in ('iadd', 'isub', 'imul', 'itruediv', 'ifloordiv', 'imod', 'ipow'))
        # Other variables
        cls.SQLebraClass = bool_
        cls.PyClass = bool
        cls.x = (True, False)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def test_steps(self):
        self._run_steps()
