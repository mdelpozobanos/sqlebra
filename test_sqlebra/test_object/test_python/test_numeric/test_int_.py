from . import Test_Numeric
from sqlebra.object.python import int_


class Test_int_(Test_Numeric):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        # Other variables
        cls.SQLebraClass = int_
        cls.PyClass = int
        cls.x = (10, 2, -100)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def test_steps(self):
        self._run_steps()
