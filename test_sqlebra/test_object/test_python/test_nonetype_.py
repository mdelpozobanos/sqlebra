from ..test_single import Test_Single
from sqlebra.object.python import NoneType_


class Test_NoneType_(Test_Single):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._steps_list = (tst
                           for tst in cls._steps_list
                           if tst not in ('int', 'float', 'invert'))
        # Other variables
        cls.SQLebraClass = NoneType_
        cls.PyClass = type(None)
        cls.x = (None, 2)

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def test_steps(self):
        self._run_steps()

    def pyclass(self):
        self.assertEqual(self.PyClass, type(self.x[0]))
        self.assertEqual(self.PyClass, self.dbo[0].pyclass)
