from .table import Table, Record
from .index import Index
from src.bits import Bits

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
        self.table.set_delete_flag(key)
        

    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        data = list(columns)
        self.table.put(self.table.db.get_next_rid(), None, data[self.table.key], Bits('11111'), data)
    """
    # Read a record with specified key
    """

    def select(self, key, column, query_columns):
        mask = ""
        for i in query_columns:
            if i == 1:
                mask+="1"
            else:
                mask+="0"
        bits_mask = Bits(mask)

        rid = self.table.key_to_baseRid(key)
        #print(key, rid)
        r = self.table.get(rid, bits_mask)
        return r


    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        data = list(columns)
        mask = Bits("")
        mask.build_from_list(columns)
        base_rid = self.table.key_to_baseRid(key)
        next_rid = self.table.db.get_next_rid()

        old_value = self.table.get(self, base_rid, mask)
        self.table.put(next_rid, base_rid, key, mask, data)
        new_value = self.table.get(self,next_rid,mask, data)
        count = 0
        for i in mask:
            if i == 1:
                self.table.index.col_btree[column].update_index(i, base_rid, old_value[count], new_value[count])
                count++


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

        res = 0
        for i in range(start_range,end_range+1):
            rid = self.table.key_to_baseRid(i)
            if rid is None:
                continue
            r = self.table.get(rid, bits_mask)
            res += r.columns[0]
        return res
