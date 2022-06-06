from sqlebra.database.dictdatabase import open_dictdatabase
from test_sqlebra.test_database.test_database import Test_Database


class Test_DictSQL(Test_Database):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

    def init(self):
        self.db = open_dictdatabase(file=self.kw_file, name=self.kw_name, mode=self.kw_mode, autocommit=False)

    def test_steps(self):
        self._run_steps()

    def _tab_name(self, name):
        """Return full name of a table. Usually incorporates the name of the database."""
        raise NotImplemented
