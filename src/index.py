from BTrees.OOBTree import OOBTreePy
from src.bits import Bits
from threading import Lock
"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""


class Index:

    def __init__(self, table):
        self.col_btree = {}
        self.table = table
        self.create_index(0)
        self._lock = Lock()


    """
    # returns the location of all records with the given value
    """

    def locate(self, value):
        return self.tree.values(value)

    """
    # optional: Create index on specific column
    """
    def is_tail_pid(self, pid):
        tpid = ((1 << 56) -1) & pid
        if tpid > ((1 << 28) - 1):
            return True
        return False

    def create_index(self, column_number):
        if column_number in self.col_btree:
            return
        print("Create index", column_number)
        zeros = [None]*self.table.num_columns
        mask = Bits("")
        mask.build_from_list(zeros[:column_number]+[1]+zeros[column_number:])
        tree = OOBTreePy()
        ks = {}
        for rid in self.table.page_directory:
            _m = self.table.page_directory[rid]
            if _m.indirection == 0 or _m.indirection > rid:
                #print("ADDING INDEX:",rid)
                r = self.table.get(rid, mask)
                #ks[rid] = r.columns[0]
                ks[r[0]] = [rid]
        tree.update(ks)
        self.col_btree[column_number] = tree
        return True

    
    def add_to_index(self, column_number, rid, value):
        self._lock.acquire()
        if not column_number in self.col_btree:
            self.create_index(column_number)
        rids = [rid]
        if self.col_btree[column_number].has_key(value) <= 0:
            self.col_btree[column_number].__setitem__(value, rids)
        else:
            #if value == 0:
            #    print("WTF", value, rids)
            rids += self.col_btree[column_number].__getitem__(value)
            self.col_btree[column_number].__setitem__(value, list(set(rids)))
        self._lock.release()
        return True
        

    def remove_from_index(self, column_number, rid, value):
        self._lock.acquire()
        if self.col_btree[column_number].has_key(value) <= 0:
            self._lock.release()
            return False
        rids = self.col_btree[column_number].__getitem__(value)
        if len(rids) == 1:
            #delete entire thing
            self.col_btree[column_number].pop(value)
        else:
            rids.remove(rid)
            self.col_btree[column_number].__setitem__(value, rids)
        self._lock.release()
        return True


    def update_index(self, column_number, rid, old_value, new_value):
        self._lock.acquire()
        # if self.col_btree[column_number].has_key(old_value) > 0:
        #     print("FALSE")
        #     return False
       # print(old_value, new_value)
        old_rids = self.col_btree[column_number].__getitem__(old_value)
        #print(old_rids)
        old_rids.remove(rid)
        new_rids = []
        if self.col_btree[column_number].has_key(new_value) > 0:
            new_rids = self.col_btree[column_number].__getitem__(new_value)
        new_rids.append(rid)
        update_dict = {}
        update_dict[old_value] = old_rids
        update_dict[new_value] = new_rids
        self.col_btree[column_number].update(update_dict)
        self._lock.release()
        return True

    def select_index(self, column_number, value):
        if self.col_btree[column_number].has_key(value) <=0:
            return []
        found_rids = self.col_btree[column_number].__getitem__(value)
        return found_rids

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, table, column_number):
        self.tree = None

    def get_range(self, l, r):
        return self.tree.values(min=l,max=r)
