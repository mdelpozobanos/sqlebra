import os
from test_sqlebra.test_init import Test_SQLebraDatabase
from .test_dictinterface import Test_DictInterface


class Test_Database(Test_SQLebraDatabase, Test_DictInterface):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._steps_list += ('init', 'file', 'name', 'info') + \
                           Test_DictInterface.setUpClass() + \
                           ('py_get', 'transaction', '*close')
        cls.kw_file = os.path.join(os.path.dirname(__file__), '..', 'files', cls.__name__ + '.mem')
        cls._try_remove(cls.kw_file)
        cls.kw_name = 'sqlebra'
        cls.kw_mode = '+'
        cls.db = None

    @classmethod
    def tearDownClass(cls) -> None:
        cls._try_remove(cls.kw_file)

    # Test public interface
    # ------------------------------------------------------------------------------------------------------------------

    def init(self):
        raise NotImplementedError

    def file(self):
        self.assertEqual(self.kw_file, self.db.file)

    def name(self):
        self.assertEqual(self.kw_name, self.db.name)

    def info(self):
        self.assertEqual("[file:'{}'][db:'{}']".format(self.kw_file, self.kw_name), self.db.info)

    def transaction(self):
        # Successful transaction
        self.db['transaction'] = False
        self.db.commit()  # Changes must be first committed
        with self.subTest(transaction='commit'):
            with self.db:
                self.db['transaction'] = True
            self.assertTrue(self.db['transaction'])
        # Failed transaction
        self.db['transaction'] = False
        self.db.commit()  # Changes must be first committed
        with self.subTest(transaction='rollback'):
            try:
                with self.db:
                    self.db['transaction'] = True
                    raise ValueError('Expected')
            except ValueError as exc:
                if exc.args[0] != 'Expected':
                    raise exc
            self.assertFalse(self.db['transaction'])

    def fix(self):
        raise NotImplementedError

    def context(self):
        pass

    def pickle(self):
        pass

    def close(self):
        self.db.close()
        with self.assertRaises(ValueError):
            self.db['x']

    def clear(self):
        self.db._clear()
