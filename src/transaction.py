from src.table import Table, Record
from src.index import Index
import threading
from random import randint
from time import time

class Transaction:

    """
    # Creates a transaction object.
    """

    def __init__(self):
        self.queries = []
        self.completed_count = 0
        self.lockedQ = []
        self.tid = int(time()*100000 + randint(0, 100))
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """

    def add_query(self, query, *args):
        argus = list(args)
        #argus.append(self.tid)
        self.queries.append((query, argus))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            #print(args)
            result, xlocked = query(self.tid, False, False, *args)
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
            query(self.tid, True, True, *args)
        return False

    def commit(self):
        # TODO: commit to database
        while self.lockedQ:
            query, args = self.lockedQ.pop()
            query(self.tid, True, False, *args)
        return True
