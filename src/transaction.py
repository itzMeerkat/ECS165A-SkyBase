from src.table import Table, Record
from src.index import Index
import threading

class Transaction:

    """
    # Creates a transaction object.
    """

    def __init__(self):
        Tid = int
        self.queries = []
        self.completed_count = 0
        # self.results = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """

    def add_query(self, query, *args):
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            # self.results.append(result)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()

            self.completed_count += 1
        return self.commit()

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        thread_id = threading.current_thread().ident
        self.rollback(thread_id)
        return False

    def commit(self):
        # TODO: commit to database
        thread_id = threading.current_thread().ident
        table.commit(thread_id)
        return True
