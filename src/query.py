from .table import Table, Record
from .index import Index
from src.bits import Bits

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """

    def __init__(self, table):
        self.table = table
        pass

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
        #schema_encoding = '0' * self.table.num_columns
        self.table.put(self.table.get_next_rid(),None,data[self.table.key],Bits('11111'),data)
        # pass

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        pass

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        data = list(columns)
        base_rid = self.table.key_to_baseRid(key)
        self.table.put(self.table.get_next_rid(),base_rid, key, Bits('11111'), data)
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
