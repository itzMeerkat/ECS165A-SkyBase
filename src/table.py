from src.page import *
from time import time
from src.column import Column, Record
from src.bits import Bits

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


    def _write_cols(self, mask, cols, dest):
        #print("writing", mask.bits)
        locs = []
        for i, v in enumerate(mask):
            if v > 0:
                pid, offset = self.columns[i].write(cols[i], dest)
                locs.append((pid, offset))
            else:
                locs.append(None)
        #print("wrote", len(locs))
        return locs

    """
    TODO: mask: string for debuging, will switch to bitmap
    mask and cols must match and fit the schema
    """
    def put(self, rid, base_rid, key, write_mask, cols):
        new_record = Record(rid, key, Bits('0' * len(cols)))

        old_locs = None

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

            base_schema_loc = base_record.locations[SCHEMA_ENCODING_COLUMN]

            base_record.mask.merge(write_mask)

            self.columns[SCHEMA_ENCODING_COLUMN].inplace_update(
                base_schema_loc[0], base_schema_loc[1], base_record.mask)

            # Merge location data
            loc_rec = None
            if not base_rid is None:
                loc_rec = base_record
            else:
                loc_rec = self.page_directory[pre_rid]
            
            old_locs = loc_rec.locations


        # Combine meta cols and data cols
        meta_and_data = new_record.meta() + cols

        # b'1111' 
        write_mask.set_meta(15)
        #print("Writing mask",write_mask.bits)
        locs = self._write_cols(write_mask, meta_and_data, dest)

        # Merge old and new locations
        if dest == TO_TAIL_PAGE:
            for i in range(len(locs)):
                if locs[i] is None:
                    locs[i] = old_locs[i]

        new_record.locations = locs
        self.page_directory[rid] = new_record


    def get(self, rid, read_mask):
        res = []
        record = self.page_directory[rid]
        latest_rec = None
        if record.mask.bits > 0:
            #print("NEED HOP", record.mask.bits)
            latest = record.get_indirection()
            latest_rec = self.page_directory[latest]

        #print("read mask", read_mask.bits, read_mask.size)
        for i in range(read_mask.size):
            col_ind = i+4
            v = read_mask[i]
            if v > 0:
                tpid = record.locations[col_ind][0]
                offset = record.locations[col_ind][1]

                # second hop needed
                if record.mask[i] > 0:
                    #print("HOP")
                    tpid = latest_rec.locations[col_ind][0]
                    offset = latest_rec.locations[col_ind][1]

                r = self.columns[col_ind].read(tpid, offset)
                res.append(r)
        return res

    
    def _get_select_num(self, mask):
        k = 0
        for i in mask:
            if i == '1':
                k += 1
        return k


    def __merge(self):
        pass
 
