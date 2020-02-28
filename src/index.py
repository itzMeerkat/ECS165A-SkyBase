from .table import Table
from BTrees.OOBTree import OOBTreePy
from src.bits import Bits

"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""


class Index:

    def __init__(self, table):
        self.col_btree = {}
        self.table = table
        create_index(0)


    """
    # returns the location of all records with the given value
    """

    def locate(self, value):
        return self.tree.values(value)

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        zeros = [None]*self.table.num_columns
        mask = Bits("")
        mask.build_from_list(zeros[:column_number]+[1]+zeros[column_number:])
        tree = OOBTreePy()
        ks = {}
        for rid in table.page_directory:
            r = table.get(rid, mask)
            #ks[rid] = r.columns[0]
            ks[r.columns[0]] = rid
        tree.update(ks)
        col_btree[column_number] = tree
        return True

    
    def add_to_index(self, column_number, rid, value):  
        if col_btree[column_number].has_key(value) == False:
            col_btree[column_number].__setitem__(value, rid)
        else:
            rids = col_btree[column_number].__getitem__(value)
            rids.append(rid)
            col_btree[column_number].__setitem__(value, rids)
        return True

    def remove_from_index(self, column_number, rid, value):
        if col_btree[column_number].has_key(value) == False:
            return False
        rids = col_btree[column_number].__getitem__(value)
        if len(rids) == 1:
            #delete entire thing
            col_btree[column_number].remove(key)
        else:
            rids.remove(rid)
            col_btree[column_number].__setitem__(value, rids)
        return True

    def update_index(self, column_number, rid, old_value, new_value):
        if col_btree[column_number].has_key(value) == False:
            return False
        old_rids = col_btree[column_number].__getitem__(old_value)
        old_rids.remove(rid)
        new_rids = col_btree[column_number].__getitem__(new_value)
        new_rids.append(rid)
        update_dict = {}
        update_dict[old_value] = old_rids
        update_dict[new_value] = new_rids
        col_btree[column_number].update(update_dict)
        return True

    def select_index(self, column_number, value):
        if col_btree[column_number].has_key(value) == False:
            return False
        found_rids = col_btree[column_number].__getitem__(value)
        return found_rids

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, table, column_number):
        self.tree = None

    def get_range(self, l, r):
        return self.tree.values(min=l,max=r)
