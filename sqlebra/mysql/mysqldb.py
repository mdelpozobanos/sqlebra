import mysql.connector
from ..database.basedb import BaseDB
from .. import exceptions as ex


class MySQLDB(BaseDB):
    """
    Class holding a MySQL database handler
    """

    def __init__(self, *args, **kwargs):
        self.connect_args = kwargs.pop('connect_args', {})
        if 'host' not in self.connect_args:
            self.connect_args['host'] = 'localhost'
        if 'user' not in self.connect_args:
            self.connect_args['user'] = 'user'
        if 'password' not in self.connect_args:
            self.connect_args['password'] = 'password'
        # if 'buffered' not in self.connect_args:
        #    self.connect_args['buffered'] = True
        super(MySQLDB, self).__init__(*args, **kwargs)

    # Python-SQL communication channel
    # ---------------------------------------------------------------

    def connect(self):
        """Connect to database"""
        if self._conx:
            raise ex.ConnectionError('Database already connected')

        # Connect to server (not database)
        self._conx = mysql.connector.connect(
            auth_plugin='mysql_native_password',
            **self.connect_args
        )
        self._c = self._conx.cursor()

        # Check if database exists
        res = self.execute("show databases like '{}'".format(self.name), fetch=True)
        if len(res) == 0:  # Create database
            self._c.execute("CREATE DATABASE {}".format(self.name))

        # Use database
        self._c.execute("USE {}".format(self.name))

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

    def _tab_name(self, name):
        return '{}.{}'.format(self.name, name)

    def execute(self, query, pars=False, fetch=False):
        """
        Execute an sql query.

        :param query: (str) SQL query
        :param pars: (list) Parameters required by the SQL query.
        :param fetch: (bool) If True, return result of execute
        :return: Result of the SQL query.
        """
        # Addapt to MySql
        if '?' in query:  # Parameter marker is %s
            query = query.replace('?', "%s")
        if 'key' in query:  # key is a reserved word
            query = query.replace('key', "`key`")
        if pars:
            if isinstance(pars[0], tuple):  # Unzip parameters
                self._c.executemany(query, [p for p in zip(*pars)])
            else:
                self._c.execute(query, pars)
        else:
            self._c.execute(query)
        if fetch:
            if self._c.description is None:
                return []
            else:
                return self._c.fetchall()
        else:  # TODO: When MySQL allows to discard unfetched results
            if self._c.description is not None:
                self._c.fetchall()

    def commit(self):
        self._conx.commit()
        return self

    def rollback(self):
        self._conx.rollback()
        return self

    def _exists_(self, tab):
        # Check if sqlebra table exists
        tabs = tab.split('.')
        return self.execute(
            "select count(*) from information_schema.tables where table_schema='{}' and table_name='{}'".format(
                tabs[0], tabs[1]
            ), fetch=True)[0][0] == 1

    def rm(self):
        """Remove database from system: i.e. delete database schema from server"""
        self.execute("drop database {}".format(self.file))
        self.disconnect()
