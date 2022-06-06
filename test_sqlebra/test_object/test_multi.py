from test_sqlebra.test_object.test_object_ import Test_object_


class Test_Multi(Test_object_):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._steps_list += ('objs', 'len', 'getitem', 'iterator')  # TODO: Check order
        cls.item = None

    # Test public interface
    # ------------------------------------------------------------------------------------------------------------------

    def objs(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(x_n, self.dbo[n].objs)

    def len(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(len(x_n), len(self.dbo[n]))

    def getitem(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(x_n[self.item], self.dbo[n][self.item])

    def iterator(self):
        for n, x_n in enumerate(self.x):
            for x1, x2 in zip(x_n, self.dbo[n]):
                self.assertEqual(x1, x2)
