from test_sqlebra.test_object.test_single import Test_Single
import math


class _Test_Numeric(Test_Single):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        # Drop non-applicable tests
        cls._steps_list += ('int', 'float',
                            'denominator', 'imag', 'numerator', 'real',
                            'lt', 'gt', 'le', 'ge',
                            'add', 'sub', 'mul', 'truediv', 'floordiv', 'mod', 'pow',
                            'iadd', 'isub', 'imul', 'itruediv', 'ifloordiv', 'imod', 'ipow',
                            'neg', 'pos', 'invert'
                            )

    def _Test_Numeric__norm_class(self):
        # Normalise class
        with self.subTest(numeric='norm_class'):
            self.xr = self.PyClass(self.xr)
            self.dbo['xr'] = self.PyClass(self.dbo['xr'])
            self.assertEqual(self.xr, self.dbo['xr'])

    # Casting
    # -------

    def int(self):
        for n, x_n in enumerate(self.x):
            try:
                x_ni = int(x_n)
            except OverflowError:
                with self.assertRaises(OverflowError):
                    int(self.dbo[n])
            except ValueError:
                with self.assertRaises(ValueError):
                    int(self.dbo[n])
            else:
                self.assertEqual(x_ni, int(self.dbo[n]))

    def float(self):
        for n, x_n in enumerate(self.x):
            if math.isnan(x_n):
                continue
            self.assertEqual(float(x_n), float(self.dbo[n]))

    # Properties
    # ----------

    def denominator(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(x_n.denominator, self.dbo[n].denominator)

    def imag(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(x_n.imag, self.dbo[n].imag)

    def numerator(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(x_n.numerator, self.dbo[n].numerator)

    def real(self):
        for n, x_n in enumerate(self.x):
            if math.isnan(x_n):
                continue
            self.assertEqual(x_n.real, self.dbo[n].real)

    # Binary operators
    # ----------------

    def add(self):
        assert self.xr == self.dbo['xr']
        with self.subTest(order='py + sqlebra'):
            self.assertEqual(self.xr + self.xr, self.xr + self.dbo['xr'])
        with self.subTest(order='sqlebra + py'):
            self.assertEqual(self.xr + self.xr, self.dbo['xr'] + self.xr)
        with self.subTest(order='sqlebra + sqlebra'):
            self.assertEqual(self.xr + self.xr, self.dbo['xr'] + self.dbo['xr'])

    def sub(self):
        assert self.xr == self.dbo['xr']
        with self.subTest(order='py - sqlebra'):
            self.assertEqual(self.xr - self.xr, self.xr - self.dbo['xr'])
        with self.subTest(order='sqlebra - py'):
            self.assertEqual(self.xr - self.xr, self.dbo['xr'] - self.xr)
        with self.subTest(order='sqlebra - sqlebra'):
            self.assertEqual(self.xr - self.xr, self.dbo['xr'] - self.dbo['xr'])

    def mul(self):
        assert self.xr == self.dbo['xr']
        with self.subTest(order='py * sqlebra'):
            self.assertEqual(self.xr * self.xr, self.xr * self.dbo['xr'])
        with self.subTest(order='sqlebra * py'):
            self.assertEqual(self.xr * self.xr, self.dbo['xr'] * self.xr)
        with self.subTest(order='sqlebra * sqlebra'):
            self.assertEqual(self.xr * self.xr, self.dbo['xr'] * self.dbo['xr'])

    def truediv(self):
        assert self.xr == self.dbo['xr']
        with self.subTest(order='py / sqlebra'):
            self.assertEqual(self.xr / self.xr, self.xr / self.dbo['xr'])
        with self.subTest(order='sqlebra / py'):
            self.assertEqual(self.xr / self.xr, self.dbo['xr'] / self.xr)
        with self.subTest(order='sqlebra / sqlebra'):
            self.assertEqual(self.xr / self.xr, self.dbo['xr'] / self.dbo['xr'])

    def floordiv(self):
        assert self.xr == self.dbo['xr']
        with self.subTest(order='py // sqlebra'):
            self.assertEqual(self.xr // self.xr, self.xr // self.dbo['xr'])
        with self.subTest(order='sqlebra // py'):
            self.assertEqual(self.xr // self.xr, self.dbo['xr'] // self.xr)
        with self.subTest(order='sqlebra // sqlebra'):
            self.assertEqual(self.xr // self.xr, self.dbo['xr'] // self.dbo['xr'])

    def mod(self):
        assert self.xr == self.dbo['xr']
        with self.subTest(order='py % sqlebra'):
            self.assertEqual(self.xr % self.xr, self.xr % self.dbo['xr'])
        with self.subTest(order='sqlebra % py'):
            self.assertEqual(self.xr % self.xr, self.dbo['xr'] % self.xr)
        with self.subTest(order='sqlebra % sqlebra'):
            self.assertEqual(self.xr % self.xr, self.dbo['xr'] % self.dbo['xr'])

    def pow(self):
        assert self.xr == self.dbo['xr']
        with self.subTest(order='py ** sqlebra'):
            self.assertEqual(self.xr ** self.xr, self.xr ** self.dbo['xr'])
        with self.subTest(order='sqlebra ** py'):
            self.assertEqual(self.xr ** self.xr, self.dbo['xr'] ** self.xr)
        with self.subTest(order='sqlebra ** sqlebra'):
            self.assertEqual(self.xr ** self.xr, self.dbo['xr'] ** self.dbo['xr'])

    def lt(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(x_n < 0, self.dbo[n] < 0)

    def gt(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(x_n > 0, self.dbo[n] > 0)

    def le(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(x_n <= 0, self.dbo[n] <= 0)

    def ge(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(x_n >= 0, self.dbo[n] >= 0)

    def iadd(self):
        assert self.xr == self.dbo['xr']
        self.xr += self.x[0]
        self.dbo['xr'] += self.x[0]
        self.assertEqual(self.xr, self.dbo['xr'])
        # Normalise class
        self._Test_Numeric__norm_class()

    def isub(self):
        assert self.xr == self.dbo['xr']
        self.xr -= self.x[0]
        self.dbo['xr'] -= self.x[0]
        self.assertEqual(self.xr, self.dbo['xr'])
        # Normalise class
        self._Test_Numeric__norm_class()

    def imul(self):
        assert self.xr == self.dbo['xr']
        self.xr *= self.x[0]
        self.dbo['xr'] *= self.x[0]
        self.assertEqual(self.xr, self.dbo['xr'])
        # Normalise class
        self._Test_Numeric__norm_class()

    def itruediv(self):
        assert self.xr == self.dbo['xr']
        self.xr /= self.x[0]
        self.dbo['xr'] /= self.x[0]
        self.assertEqual(self.xr, self.dbo['xr'])
        # Normalise class
        self._Test_Numeric__norm_class()

    def ifloordiv(self):
        assert self.xr == self.dbo['xr']
        self.xr //= self.x[0]
        self.dbo['xr'] //= self.x[0]
        self.assertEqual(self.xr, self.dbo['xr'])
        # Normalise class
        self._Test_Numeric__norm_class()

    def imod(self):
        assert self.xr == self.dbo['xr']
        self.xr %= self.x[0]
        self.dbo['xr'] %= self.x[0]
        self.assertEqual(self.xr, self.dbo['xr'])
        # Normalise class
        self._Test_Numeric__norm_class()

    def ipow(self):
        assert self.xr == self.dbo['xr']
        self.xr **= self.x[0]
        self.dbo['xr'] **= self.x[0]
        self.assertEqual(self.xr, self.dbo['xr'])
        # Normalise class
        self._Test_Numeric__norm_class()

    def neg(self):
        for n, x_n in enumerate(self.x):
            if math.isnan(x_n):
                continue
            self.assertEqual(-x_n, -self.dbo[n])

    def pos(self):
        for n, x_n in enumerate(self.x):
            if math.isnan(x_n):
                continue
            self.assertEqual(+x_n, +self.dbo[n])

    def invert(self):
        for n, x_n in enumerate(self.x):
            self.assertEqual(~x_n, ~self.dbo[n])
