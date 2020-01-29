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


    def _write_cols(self, mask, cols):
        locs = []
        for i, v in enumerate(mask):
            if v == '1':
                pid, offset = self.columns[i].write(cols[i], dest)
                locs.append((pid, offset))
        return locs

    """
    TODO: mask: string for debuging, will switch to bitmap
    mask and cols must match and fit the schema
    """
    def put(self, rid, base_rid, key, mask, cols):
        new_record = Record(rid, key, mask)

        dest = TO_TAIL_PAGE
        if base_rid is None:
            dest = TO_BASE_PAGE
        else:
            base_record = self.page_directory[base_rid]
            pre_rid = base_record.get_indirection()
            new_record.set_indirection(pre_rid)
            base_record.set_indirection(rid)

            # Inplace update base record indirection column
            base_ind_loc = base_record.locations[INDIRECTION_COLUMN]
            self.columns[INDIRECTION_COLUMN].inplace_update(base_ind_loc[0], base_ind_loc[1], base_record.get_indirection())

        # Combine meta cols and data cols
        meta_and_data = new_record.meta() + cols
        mask = '1' * META_COL_SIZE + mask

        locs = self._write_cols(mask, meta_and_data)
        new_record.locations = locs
        self.page_directory[rid] = new_record


    def get(self, rid, mask):
        col_num = self._get_select_num(mask) # we can use this to pre-allocate mem to improve performance
        res = []
        for i, v in enumerate(mask):
            if v == '1':
                tpid, offset = self.page_directory[rid]
                r = self.columns[i + META_COL_SIZE].read(tpid, offset)
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
 
