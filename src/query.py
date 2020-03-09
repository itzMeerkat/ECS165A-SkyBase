from .table import Table, Record
from .index import Index
from src.bits import Bits

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

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        rids = self.table.index.select_index(self.table.key, key)

        # Try to acquire LOCK
        for i,r in enumerate(rids):
            LOCK_ACQUIRED = self.table.db.rid_lock.acquire(r)
            if not LOCK_ACQUIRED:
                for j in range(i):
                    self.table.db.rid_lock.release(rids[j])
                return False

        for i in rids:
            self.table.index.remove_from_index(self.table.key, i,key)
            self.table.delete(i)
        return True

    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        data = list(columns)

        next_rid = self.table.db.get_next_rid()
        self.table.put(next_rid, None, data[self.table.key], Bits('1'*len(data)), data)
        for col in range(self.table.num_columns):
            self.table.index.add_to_index(col, next_rid, data[col])
        return True


    def select(self, key, column, query_columns):
        found_records = Wrapper()
        mask = ""
        for i in query_columns:
            if i == 1:
                mask+="1"
            else:
                mask+="0"
        bits_mask = Bits(mask)

        rids = self.table.index.select_index(column, key)

        if len(rids) <= 0:
            return False

        for r in rids:
            found_records.additem(QueryResult(self.table.get(r, bits_mask)))

        return found_records



    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        data = list(columns)
        mask = Bits("")
        mask.build_from_list(data)

        base_rid = self.table.index.select_index(self.table.key, key)[0]

        # Try to acquire LOCK
        LOCK_ACQUIRED = self.table.db.rid_lock.acquire(base_rid)
        if not LOCK_ACQUIRED:
            return False

        next_rid = self.table.db.get_next_rid()
        old_value = self.table.get(base_rid, mask)
        self.table.put(next_rid, base_rid, key, mask, data)
        mask = Bits("")
        mask.build_from_list(columns)
        _columns=[i for i in columns if not i is None]
        
        count = 0
        for i in mask:
            if i == 1:
                self.table.index.update_index(
                    i, base_rid, old_value[count], _columns[count])
                count+= 1
        # TODO: Release lock? or release after transcation done
        return True


    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
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
        return res

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
