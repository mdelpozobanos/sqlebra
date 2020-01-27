import os
import sqlite3
import pydash
from ..database.basedb import BaseDB
from .. import exceptions as ex


class SQLiteDB(BaseDB):
    """
    Class holding a SQLite database handler
    """

    def __init__(self, *args, **kwargs):
        self.connect_args = kwargs.pop('connect_args', {})
        super(SQLiteDB, self).__init__(*args, **kwargs)

    # Python-SQL communication channel
    # ---------------------------------------------------------------

    def connect(self):
        """Connect to database"""
        if self._conx:
            raise ex.ConnectionError('Database already connected')
        if 'file_options' in self.connect_args:
            file = 'file:{}?{}'.format(self.file, self.connect_args['file_options'])
        else:
            file = self.file
        # self._conx = sqlite3.connect(file, isolation_level=None, **pydash.omit(kwargs, 'file_options'))
        self._conx = sqlite3.connect(file, **pydash.omit(self.connect_args, 'file_options'))
        self._c = self._conx.cursor()
        return self

    def disconnect(self):
        if not self._conx:
            raise ex.ConnectionError('Database not connected')
        self._c.close()
        self._conx.close()
        self._conx = None
        self._c = None
        return self

    # Simplified SQL interface
    # ---------------------------------------------------------

    def execute(self, query, pars=False):
        """
        Execute an sql query.

        :param query: (str) SQL query
        :param pars: (list) Parameters required by the SQL query.
        :return: Result of the SQL query.
        """
        # query = query.replace('[db]', self.name)
        if pars:
            return self._c.execute(query, pars).fetchall()
        else:
            return self._c.execute(query).fetchall()

    def commit(self):
        self._conx.commit()
        return self

    def rollback(self):
        self._conx.rollback()
        return self

    def exists(self):
        return self.execute(
            "select count(*) from sqlite_master where type='table' and name='{}'".format(self.name))[0][0] == 1

    def rm(self):
        """Remove database from system: i.e. delete SQLite database file"""
        self.disconnect()
        os.remove(self.file)
