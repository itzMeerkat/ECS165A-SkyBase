from src.page import *
from time import time
from src.column import Column, Record
from src.bits import Bits
from .bufferpool import Bufferpool
from .index import Index
from .lock_manager import *


#import traceback

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key, file_handler,page_directory,reverse_ind,db):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.db = db
        self.bufferpool = Bufferpool(file_handler)
        self.columns = [Column(self.bufferpool,i+1) for i in range(self.num_columns + META_COL_SIZE)]
        # {rid: Record obj}
        self.page_directory = page_directory
        self.reverse_indirection = reverse_ind
        self.rid = 1
        self.deleted_base_rid = []
        self.index = Index(self)

    def get_next_rid(self):
        r = self.rid
        self.rid += 1
        return r


    def _write_cols(self, mask, cols, dest, bid):
        #print("writing", mask.bits)
        locs = []
        ofs = None
        l = mask.size
        for i in range(l):
            bpid = None
            if dest == TO_TAIL_PAGE:
                bpid = bid[i]
            pid, ofs = self.columns[i].write(cols[i], dest, bpid)
            locs.append(pid)
        #print("wrote", len(locs))
        return locs, ofs


    def put(self, rid, base_rid, key, write_mask, cols):
        l = self.num_columns
        new_record = Record(rid, key, Bits('0' * l))
        dest = TO_TAIL_PAGE

        if base_rid is None:
            dest = TO_BASE_PAGE
        else:
            base_record = self.page_directory[base_rid]
            pre_rid = base_record.indirection
            if pre_rid == 0:
                pre_rid = base_rid


            new_record.indirection = pre_rid
            base_record.indirection = rid

            base_ind_pid = base_record.pids[INDIRECTION_COLUMN]
            base_offset = base_record.offset
            self.columns[INDIRECTION_COLUMN].inplace_update(
                base_ind_pid, base_offset, rid)

            base_schema_loc = base_record.pids[SCHEMA_ENCODING_COLUMN]

            base_record.mask.merge(write_mask)

            self.columns[SCHEMA_ENCODING_COLUMN].inplace_update(
                base_schema_loc, base_offset, base_record.mask)


        base_pids = None
        if not base_rid is None:
            base_pids = self.page_directory[base_rid].pids

        if dest == TO_TAIL_PAGE:
            for i in range(l):
                if cols[i] is None:
                    _pre_pid = self.page_directory[pre_rid].pids[i + 4]
                    _pre_offset = self.page_directory[pre_rid].offset
                    cols[i] = self.columns[i+4].read(_pre_pid, _pre_offset)

        meta_and_data = new_record.meta() + cols
        write_mask.set_meta(15)

        locs, offset = self._write_cols(
            write_mask, meta_and_data, dest, base_pids)
        new_record.pids = locs
        new_record.offset = offset

        self.page_directory[rid] = new_record
        self.reverse_indirection[rid] = base_rid


    def get(self, rid, read_mask):
        res = []
        record = self.page_directory[rid]
        latest_rec = None

        if record.mask.bits > 0:
            latest = record.indirection
            latest_rec = self.page_directory[latest]

        for i in range(read_mask.size):
            col_ind = i + 4
            v = read_mask[i]
            if v > 0:
                tpid = record.pids[col_ind]
                offset = record.offset

                if record.mask[i] > 0:
                    tpid = latest_rec.pids[col_ind]
                    offset = latest_rec.offset

                r = self.columns[col_ind].read(tpid, offset)
                res.append(r)
        
        return res


    def delete(self, rid):
        m=Bits("1"*self.num_columns)
        self.put(0, rid, 0, m, [0] * self.num_columns)
    
    def __merge(self):
        pass

    def update_page_directory(self):
        pass
 
