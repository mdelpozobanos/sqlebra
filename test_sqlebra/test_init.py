import os
import unittest


class Test_SQLebra(unittest.TestCase):

    @staticmethod
    def _try_remove(file_name):
        try:
            os.remove(file_name)
        except FileNotFoundError:
            pass

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._steps_list = ()
        cls._been_here = []
        # cls.db = None  # Property holding the SQLebra database

    def _steps(self):
        for name in self._steps_list:
            yield name, getattr(self, name.replace('*', ''))

    def _run_steps(self):
        for name, step in self._steps():
            with self.subTest(method=name):
                step()
                if name[0] != '*':
                    self.db.check()

    # Add the following in the top level class
    # def test_steps(self):
    #     self._run_steps()


class Test_SQLebraDatabase(Test_SQLebra):
    pass


class Test_SQLebraObject(Test_SQLebra):
    pass
