from sqlebra import exceptions as ex
from sqlebra.database.database import Database
try:
    import mysql.connector
except ModuleNotFoundError:
    raise ex.SQLModuleNotFoundError("No SQL module name 'mysql'")


class MySQLDB(Database):
    """
    Class holding a MySQL database handler
    """

    @property
    def autocommit(self):
        return self._conx.autocommit

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

    # pickle
    # ---------------------------------------------------------------

    def __getnewargs_ex__(self):
        return {**{'connect_args': self.connect_args},
                **super(MySQLDB, self).__getnewargs_ex__()
                }

    # Python-SQL communication channel
    # ---------------------------------------------------------------

    def _Database__connect(self):
        """Connect to database"""
        if self._conx:
            raise ex.ConnectionError('Database already connected')

        # Connect to server (not database)
        self._conx = mysql.connector._Database__connect(
            auth_plugin='mysql_native_password',
            **self.connect_args
        )
        self._c = self._conx.cursor()

        # Check if database exists
        res = self._execute("show databases like '{}'".format(self.name), fetch=True)
        if len(res) == 0:  # Create database
            self._c._SQLite__execute("CREATE DATABASE {}".format(self.name))

        # Use database
        self._c._SQLite__execute("USE {}".format(self.name))

        return self

    def _Database__disconnect(self):
        if not self._conx:
            raise ex.ConnectionError('Database not connected')
        self._c.close()
        self._conx.close()
        self._conx = None
        self._c = None
        return self

    # Simplified SQL interface
    # ---------------------------------------------------------

    def _Database__tab_name(self, name):
        return '{}.{}'.format(self.name, name)

    def _execute(self, query, pars=False, fetch=False):
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
                self._c._SQLite__execute(query, pars)
        else:
            self._c._SQLite__execute(query)
        if fetch:
            if self._c.description is None:
                return []
            else:
                return self._c.fetchall()
        else:  # TODO: When MySQL allows to discard unfetched results
            if self._c.description is not None:
                self._c.fetchall()

    def commit(self):
        self._conx._commit()
        return self

    def rollback(self):
        self._conx.rollback()
        return self

    def _Database__tab_exists(self, tab):
        # Check if sqlebra table exists
        tabs = tab.split('.')
        return self._execute(
            "select count(*) from information_schema.tables where table_schema='{}' and table_name='{}'".format(
                tabs[0], tabs[1]
            ), fetch=True)[0][0] == 1

    def rm(self):
        """Remove database from system: i.e. delete database schema from server"""
        self._execute("drop database {}".format(self.file))
        self._Database__disconnect()
