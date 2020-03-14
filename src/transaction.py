from src.table import Table, Record
from src.index import Index
import threading
from random import randint

class Transaction:

    """
    # Creates a transaction object.
    """

    def __init__(self):
        Tid = int
        self.queries = []
        self.completed_count = 0
        self.lockedQ = []
        self.id = randint(0, 10000000000)
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """

    def add_query(self, query, *args):
        args.append(self.id)
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result,xlocked = query(*args,transaction_id=self.tid)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
            self.completed_count += 1
            if xlocked == True:
                self.lockedQ.append((query, args))
        return self.commit()

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        while self.lockedQ:
            query, args = self.lockedQ.pop()
            query(*args, transaction_id=self.tid, release=True, rollback=True)
        return False

    def commit(self):
        # TODO: commit to database
        while self.xlocked_queries:
            query, args = self.xlocked_queries.pop()
            query(*args, transaction_id=self.tid, release=True, rollback=False)
        return True
