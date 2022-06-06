from sqlebra import SQLebraDatabase


class Transaction(SQLebraDatabase):
    """Database transaction class handling nested transactions"""

    db = None

    def __init__(self):
        pass

    def __getattr__(self, item):
        if item == 'db':
            return AttributeError()
        return getattr(self.db, item)

    def transaction(self):
        """Return a database transaction"""
        return self

    # with
    # ---------------------------------------------------------------

    def __enter__(self):
        self.db._Database__transaction_level += 1
        if self.db._Database__transaction_level == 1:
            self.db._SQLite__execute('begin')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db._Database__transaction_level -= 1
        if exc_type:
            self.rollback()
            exc_type(exc_val, exc_tb)
        elif self._Database__transaction_level == 0:
            self.commit()
