from . import Test_numpy
import numpy as np
from sqlebra.object.numpy import ndarray_


class Test_ndarray_(Test_numpy):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._steps_list = tuple(s for s in cls._steps_list if s != 'bool')
        cls._steps_list += ('setitem', 'any', 'all', 'getitem_slice', 'setitem_slice', 'flatten', 'tolist', 'squeeze')
        # Other variables
        cls.SQLebraClass = ndarray_
        cls.PyClass = np.ndarray
        cls.x = (np.array([0, 1.1, '2', True, None]),
                 np.array([[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10, 11]]))
        cls.x_none = np.array([[0, 0.0, None], [False, 0, 0.0]])
        cls.x_numeric = np.array([
            [
                [0, 1], [2, 3], [4, 5]
            ], [
                [6, 7], [8, 9], [10, 11]
            ], [
                [12, 13], [14, 15], [16, 17]
            ], [
                [18, 19], [20, 21], [22, 23]
            ]])
        cls.x_structured = np.array([(0, 0.0, 'a'),
                                     (1, 1.1, 'b'),
                                     (2, 2.2, 'c'),
                                     (3, 3.3, 'd')],
                                    dtype=[('col0', int), ('col1', float), ('col2', str)])
        cls.item = 0

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def init(self):
        super().init()
        self.dbo['x_none'] = self.x_none
        self.dbo['x_numeric'] = self.x_numeric
        self.dbo['x_structured'] = self.x_structured
        # TODO: Add tests for structured array

    def py_get(self):
        for n, x_n in enumerate(self.x):
            np.testing.assert_array_equal(x_n, self.dbo[n].py)

    def py_set(self):
        with self.subTest(with_='new value'):
            self.xr = self.x[1].copy()
            self.dbo['xr'].py = self.x[1]
            x = self.xr
            y = self.dbo['xr']
            self.assertEqual(x.shape, y.shape)
            self.assertEqual(x.dtype, y.dtype)
            self.assertEqual(x.size, y.size)
            self.assertEqual(x.ndim, y.ndim)
            np.testing.assert_array_equal(x, y)
        with self.subTest(with_='itself'):
            self.dbo['xr'].py = self.dbo['xr']
            x = self.xr
            y = self.dbo['xr']
            self.assertEqual(x.shape, y.shape)
            self.assertEqual(x.dtype, y.dtype)
            self.assertEqual(x.size, y.size)
            self.assertEqual(x.ndim, y.ndim)
            np.testing.assert_array_equal(x, y)
        with self.subTest(with_='with SQLebra object'):
            self.xr = self.x[0].copy()
            self.dbo['xr'].py = self.dbo[0]
            x = self.xr
            y = self.dbo['xr']
            self.assertEqual(x.shape, y.shape)
            self.assertEqual(x.dtype, y.dtype)
            self.assertEqual(x.size, y.size)
            self.assertEqual(x.ndim, y.ndim)
            np.testing.assert_array_equal(x, y)

    def eq(self):
        for n, x_n in enumerate(self.x):
            np.testing.assert_array_equal(x_n, self.dbo[n])

    def objs(self):
        for n, x_n in enumerate(self.x):
            np.testing.assert_array_equal(x_n, self.dbo[n].objs)

    def test_steps(self):
        self._run_steps()

    def getitem(self):
        for n, x_n in enumerate(self.x):
            np.testing.assert_array_equal(x_n[self.item], self.dbo[n][self.item])

    def setitem(self):
        # Set item
        self.xr[3] = 'setitem'
        self.dbo['xr'][3] = 'setitem'
        np.testing.assert_array_equal(self.xr, self.dbo['xr'])

    def any(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(x_n.any(), self.dbo[n].any())
        self.assertEqual(self.x_none.any(), self.dbo['x_none'].any())

    def all(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(x_n.all(), self.dbo[n].all())
        self.assertEqual(self.x_none.all(), self.dbo['x_none'].all())
        self.assertEqual(self.x_numeric.all(), self.dbo['x_numeric'].all())

    def getitem_slice(self):
        with self.subTest(slice_from='vector'):
            x = self.x[0][:2]
            y = self.dbo[0][:2]
            self.assertEqual(x.shape, y.shape)
            self.assertEqual(x.dtype, y.dtype)
            self.assertEqual(x.size, y.size)
            self.assertEqual(x.ndim, y.ndim)
            np.testing.assert_array_equal(x, y)
        with self.subTest(slice_from='matrix'):
            x = self.x[1][:2]
            y = self.dbo[1][:2]
            self.assertEqual(x.shape, y.shape)
            self.assertEqual(x.dtype, y.dtype)
            self.assertEqual(x.size, y.size)
            self.assertEqual(x.ndim, y.ndim)
            np.testing.assert_array_equal(x, y)
        with self.subTest(slice_from='matrix2'):
            x = self.x[1][:-1, ::2]
            y = self.dbo[1][:-1, ::2]
            self.assertEqual(x.shape, y.shape)
            self.assertEqual(x.dtype, y.dtype)
            self.assertEqual(x.size, y.size)
            self.assertEqual(x.ndim, y.ndim)
            np.testing.assert_array_equal(x, y)
        with self.subTest(slice_from='matrix3'):
            x = self.x_numeric[[0, 3, 3, 3, 2], ::2, None, [True, False]]
            y = self.dbo['x_numeric'][[0, 3, 3, 3, 2], ::2, None, [True, False]]
            self.assertEqual(x.shape, y.shape)
            self.assertEqual(x.dtype, y.dtype)
            self.assertEqual(x.size, y.size)
            self.assertEqual(x.ndim, y.ndim)
            np.testing.assert_array_equal(x, y)
        with self.subTest(slice_from='slice'):
            x = self.x_numeric[:-1, ::2, 0][1:, :-1]
            y = self.dbo['x_numeric'][:-1, ::2, 0][1:, :-1]
            self.assertEqual(x.shape, y.shape)
            self.assertEqual(x.dtype, y.dtype)
            self.assertEqual(x.size, y.size)
            self.assertEqual(x.ndim, y.ndim)
            np.testing.assert_array_equal(x, y)

    def setitem_slice(self):
        with self.subTest(slice_from='vector'):
            # Set value
            self.xr = self.x[0].copy()
            self.dbo['xr'] = self.x[0]
            # Set slice
            self.xr[1:3] = 0
            self.dbo['xr'][1:3] = 0
            np.testing.assert_array_equal(self.xr, self.dbo['xr'])
        with self.subTest(slice_from='matrix'):
            # Set value
            self.xr = self.x[1].copy()
            self.dbo['xr'] = self.x[1]
            # Set slice
            self.xr[1:3] = 1
            self.dbo['xr'][1:3] = 1
            np.testing.assert_array_equal(self.xr, self.dbo['xr'])
        with self.subTest(slice_from='matrix2'):
            # Set value
            self.xr = self.x[1].copy()
            self.dbo['xr'] = self.x[1]
            # Set slice
            self.xr[:2, 1:] = 2
            self.dbo['xr'][:2, 1:] = 2
            np.testing.assert_array_equal(self.xr, self.dbo['xr'])

    def iterator(self):
        with self.subTest(iterate_through='vector'):
            for i1, i2 in zip(self.x[0], self.dbo[0]):
                self.assertEqual(i1, i2)
        with self.subTest(iterate_through='matrix'):
            for i1, i2 in zip(self.x[1], self.dbo[1]):
                np.testing.assert_array_equal(i1, i2)

    def flatten(self):
        with self.subTest(list_from='ndarray'):
            for n, x_n in enumerate(self.x):
                x = x_n.flatten()
                y = self.dbo[n].flatten()
                self.assertEqual(x.shape, y.shape)
                self.assertEqual(x.dtype, y.dtype)
                self.assertEqual(x.size, y.size)
                self.assertEqual(x.ndim, y.ndim)
                np.testing.assert_array_equal(x, y)
        with self.subTest(list_from='ndarrayView'):
            for n, x_n in enumerate(self.x):
                x = x_n[1:-1].flatten()
                y = self.dbo[n][1:-1].flatten()
                self.assertEqual(x.shape, y.shape)
                self.assertEqual(x.dtype, y.dtype)
                self.assertEqual(x.size, y.size)
                self.assertEqual(x.ndim, y.ndim)
                np.testing.assert_array_equal(x, y)

    def tolist(self):
        with self.subTest(list_from='ndarray'):
            for n, x_n in enumerate(self.x):
                self.assertEqual(x_n.tolist(), self.dbo[n].tolist())
        with self.subTest(list_from='ndarrayView'):
            for n, x_n in enumerate(self.x):
                self.assertEqual(x_n[1:-1].tolist(), self.dbo[n][1:-1].tolist())

    def squeeze(self):
        x = self.x_numeric[0, :, None].squeeze()
        y = self.dbo['x_numeric'][0, :, None].squeeze()
        self.assertEqual(x.shape, y.shape)
        self.assertEqual(x.dtype, y.dtype)
        self.assertEqual(x.size, y.size)
        self.assertEqual(x.ndim, y.ndim)
        np.testing.assert_array_equal(x, y)