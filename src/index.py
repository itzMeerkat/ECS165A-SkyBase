from .table import Table
from BTrees.OOBTree import OOBTreePy
from src.bits import Bits

"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""


class Index:

    def __init__(self, table):
        self.tree = None

    """
    # returns the location of all records with the given value
    """

    def locate(self, value):
        return self.tree.values(value)

    """
    # optional: Create index on specific column
    """

    def create_index(self, table, column_number):
        zeros = [None]*table.num_columns
        mask = Bits("")
        mask.build_from_list(zeros[:column_number]+[1]+zeros[column_number:])

        self.tree = OOBTreePy()

        ks = {}
        for rid in table.page_directory:
            r = table.get(rid, mask)
            ks[rid] = r.columns[0]
        
        self.tree.update(ks)
        


    """
    # optional: Drop index of specific column
    """

    def drop_index(self, table, column_number):
        self.tree = None

    def get_range(self, l, r):
        return self.tree.values(min=l,max=r)
