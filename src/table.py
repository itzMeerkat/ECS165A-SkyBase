from src.page import *
from time import time
from src.column import Column, Record


class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns

        self.columns = [Column()] * (self.num_columns + META_COL_SIZE)

        # {rid: Record obj}
        self.page_directory = {}


    """
    TODO: mask: string for debuging, will switch to bitmap
    mask and cols must match and fit the schema
    """
    def put(self, rid, base_rid, key, mask, cols):
        new_record = Record(rid, key, mask)

        dest = TO_TAIL_PAGE
        if base_rid is None:
            dest = TO_BASE_PAGE
        
        # Actual writing
        for i, v in enumerate(mask):
            if v == '1':
                pid, offset = self.columns[i].write(cols[i], dest)
                new_record.append_col_addr(pid, offset)
        
        
        # Maintain indirection pointer
        if not base_rid is None:
            base_record = self.page_directory[base_rid]
            pre_rid = base_record.get_indirection()
            new_record.set_indirection(pre_rid)
            base_record.set_indirection(rid)

        self.page_directory[rid] = new_record


    def get(self, rid, mask):
        col_num = self._get_select_num(mask) # we can use this to pre-allocate mem to improve performance
        res = []
        for i, v in enumerate(mask):
            if v == '1':
                tpid, offset = self.page_directory[rid]
                r = self.columns[i].read(tpid, offset)
                res.append(r)
        return r

    
    def _get_select_num(self, mask):
        k = 0
        for i in mask:
            if i == '1':
                k += 1
        return k


    def __merge(self):
        pass
 
