from . import Test_Sequence
from sqlebra.object.python import tuple_


class Test_list_(Test_Sequence):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._steps_list += ()
        # Other variables
        cls.SQLebraClass = tuple_
        cls.PyClass = tuple
        cls.x = ((0, '1', 2.3, True, None, 0, 10, 1.1, 9, 10),
                 (0, '11', 12.3, None, False, 0, 10, 1.1, 9, 10),
                 (0, 1, (0, 1, (0, 1))))
        cls.x_none = (0, 0.0, False, '', None)
        cls.x_numeric = (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def test_steps(self):
        self._run_steps()
