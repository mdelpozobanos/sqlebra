
class BaseTransaction:
    """Database transaction class handling nested transactions"""

    def __init__(self, db):
        self.db = db

    def __getattr__(self, item):
        return getattr(self.db, item)

    def transaction(self):
        """Return a database transaction"""
        return self

    # with
    # ---------------------------------------------------------------

    def __enter__(self):
        self.db._transaction_level += 1
        if self.db._transaction_level == 1:
            self.db.execute('begin')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db._transaction_level -= 1
        if exc_type:
            self.rollback()
            exc_type(exc_val, exc_tb)
        elif self._transaction_level == 0:
            self.commit()
