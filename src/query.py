from .table import Table, Record
from .index import Index
from .log import logger_inst
from src.bits import Bits
from .lock_manager import *

class Wrapper:
    def __init__(self):
        self.inlist = []
    def additem(self, qr):
        self.inlist.append(qr)
    def __getitem__(self, key):
        return self.inlist[key]
    def __contains__(self, item):
        return any([qr.equals(item) for qr in self.inlist])
    def __len__(self):
        return len(self.inlist)

class QueryResult:
    def __init__(self, res):
        self.columns = res
    def add_result(self, r):
        self.columns.append(r)
    def equals(self, item):
        if (len(item)!=len(self)):
            return False
        for i, val in enumerate(self.columns):
            if(val != item[i]):
                return False
        return True
    def __getitem__(self, key):
        return self.columns[key]
    def __str__(self):
        return str(self.columns)
    def __len__(self):
        return len(self.columns)

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """

    def __init__(self, table):
        self.table = table
        self.logger = logger_inst

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, transaction_id, release, rollback, key):
        #need to get values of the record before we delete it
        base_rid = self.table.index.select_index(self.table.key, key)[0]
        old_value = self.table.get(base_rid, Bits('1'*self.table.num_columns))

        if transaction_id is not None:
            send_query = [transaction_id, ["delete", key], (old_value)]
            self.logger.first_add(send_query)

        rids = self.table.index.select_index(self.table.key, key)
        # Try to acquire LOCK

        # Try to acquire write_lock
        for i in rids:
            self.table.index.remove_from_index(self.table.key, i,key)
            self.table.delete(i)

        if transaction_id is not None:
            self.logger.finished_add(send_query)
        return True

    """
    # Insert a record with specified columns
    """

    def insert(self, transaction_id, release, rollback, *columns):
        #print("Insert")
        data = list(columns)
        next_rid = self.table.db.get_next_rid()
        #print("Rid",next_rid)
        if transaction_id is not None:
            send_query = [transaction_id, ["insert"], (next_rid, data)]
            self.logger.first_add(send_query)

        self.table.put(next_rid, None, data[self.table.key], Bits('1'*len(data)), data)

        for col in range(self.table.num_columns):
            self.table.index.add_to_index(col, next_rid, data[col])
        
        if transaction_id is not None:
            self.logger.finished_add(send_query)
        return True

    def select(self, transaction_id, release, rollback, key, column, query_columns):
        
        # if transaction_id is not None:
        #     send_query = [transaction_id, ["select", key, column, query_columns], None]
        #     self.logger.first_add(send_query)

        found_records = Wrapper()
        mask = ""
        for i in query_columns:
            if i == 1:
                mask+="1"
            else:
                mask+="0"
        bits_mask = Bits(mask)

        rids = self.table.index.select_index(column, key)
        
        #try to acquire read_lock
        
        for r in rids:
            LOCK_ACQUIRED = self.table.db.lock_manager.acquire_read(r,transaction_id) #we need tid here
            if not LOCK_ACQUIRED:
                return False, False

        if len(rids) <= 0:
            return False, False

        for r in rids:
            found_records.additem(QueryResult(self.table.get(r, bits_mask)))

        # if transaction_id is not None:
        #     self.logger.finished_add(send_query)
        #     return found_records, False
        
        return found_records, False


    """
    # Update a record with specified key and columns
    """

    def update(self, transaction_id, release, rollback, key, *columns):
        data = list(columns)
        mask = Bits("")
        mask.build_from_list(data)
        base_rid = self.table.index.select_index(self.table.key, key)[0]
        
        #Commit/Abort
        if release:
            #Abort
            if rollback:
                #find previous version in log and update it
                send_query = [transaction_id, ["Update"], (base_rid, data)]
                datafromLOG = self.logger.find_aborted(send_query)
                #append the very first, oldest value to the update.
                next_rid = self.table.db.get_next_rid()
                data = datafromLOG
                #self.table.rollback(key)
                
            #release all the locks
            self.table.db.lock_manager.release_write(base_rid, transaction_id)
            return 
        else: 
            # Try to acquire write_lock
            LOCK_ACQUIRED = self.table.db.lock_manager.acquire_write(base_rid,transaction_id)
            if not LOCK_ACQUIRED:
                #abort this transaction
                print("Acquire lock fail")
                return False,False
            
            next_rid = self.table.db.get_next_rid()
            old_value = self.table.get(base_rid, mask)
            all_old_values = self.table.get(base_rid, Bits('1'*self.table.num_columns))
            if transaction_id is not None:
                send_query = [transaction_id, ["update", key, data], (all_old_values, data)]
                self.logger.first_add(send_query)

            self.table.put(next_rid, base_rid, key, mask, data)
            mask = Bits("")
            mask.build_from_list(columns)
            _columns=[i for i in columns if not i is None]
            #print(old_value, _columns, mask.bits, base_rid)
            count = 0
            for i in mask:
                if i == 1:
                    self.table.index.update_index(
                        i, base_rid, old_value[count], _columns[count])
                    count+= 1
            # TODO: Release lock? or release after transcation done
            if transaction_id is not None:
                self.logger.finished_add(send_query)
            return True,True


    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, transaction_id, release, rollback, start_range, end_range, aggregate_column_index):
        if transaction_id is not None:
            send_query = [transaction_id, ["sum",start_range, end_range, aggregate_column_index], None]
            self.logger.first_add(send_query)

        _m = [0]*self.table.num_columns
        _m[aggregate_column_index] = 1
        mask = ""
        for i in _m:
            if i == 1:
                mask += "1"
            else:
                mask += "0"
        bits_mask = Bits(mask)

        rids = self.table.index.col_btree[self.table.key].values(start_range, end_range)
        if len(rids) <= 0:
            return False
        res = 0
        for i in rids:
            r = self.table.get(i[0], bits_mask)
            res += r[0]

        if transaction_id is not None:
            self.logger.finished_add(send_query)
        return res

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column, transaction_id=None):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            if transaction_id is not None:
                send_query = [transaction_id, ["increment", key, column], (r[column], r[column] + 1)]
                self.logger.first_add(send_query)

            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)

            if transaction_id is not None:
                self.logger.finished_add(send_query)
            return u
        return False
