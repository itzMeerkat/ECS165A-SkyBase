from src.page import *
from time import time
from src.column import Column, Record
from src.bits import Bits
from .bufferpool import Bufferpool

class QueryResult:
    def __init__(self, res):
        self.columns = res
    
    def __getitem__(self, key):
        if key==0:
            return self
        return None
    def __str__(self):
        return str(self.columns)

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
        self.columns = [Column(self.bufferpool, i+1) for i in range(self.num_columns + META_COL_SIZE)]
        # {rid: Record obj}
        self.page_directory = page_directory
        self.reverse_indirection = reverse_ind
        self.rid = 1
        self.fake_index = {}
        self.deleted_base_rid = []
        self.index = Index()

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

    """
    TODO: mask: string for debuging, will switch to bitmap
    mask and cols must match and fit the schema
    """
    def put(self, rid, base_rid, key, write_mask, cols):
        l = len(cols)
        new_record = Record(rid, key, Bits('0' * l))
        dest = TO_TAIL_PAGE

        if base_rid is None:
            dest = TO_BASE_PAGE
            self.fake_index[key] = rid
        else:
            base_record = self.page_directory[base_rid]
            #old_loc = base_record.locations
            pre_rid = base_record.indirection
            if pre_rid == 0:
                pre_rid = base_rid


            new_record.indirection = pre_rid
            base_record.indirection = rid

            # Inplace update base record indirection column
            base_ind_pid = base_record.pids[INDIRECTION_COLUMN]
            base_offset = base_record.offset
            self.columns[INDIRECTION_COLUMN].inplace_update(
                base_ind_pid, base_offset, base_record.indirection)

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

        # Combine meta cols and data cols
        meta_and_data = new_record.meta() + cols
        # b'1111' 
        write_mask.set_meta(15)
        #print(cols)
        locs, offset = self._write_cols(
            write_mask, meta_and_data, dest, base_pids)

        # Merge old and new locations
        new_record.pids = locs
        new_record.offset = offset
        self.page_directory[rid] = new_record
        self.reverse_indirection[rid] = base_rid


    def get(self, rid, read_mask):
        res = []
        record = self.page_directory[rid]
        latest_rec = None
        if record.mask.bits > 0:
            #print("NEED HOP", record.mask.bits)
            latest = record.indirection
            latest_rec = self.page_directory[latest]

        #print("read mask", read_mask.bits, read_mask.size)
        #print("Mask size", read_mask.size)
        for i in range(read_mask.size):
            col_ind = i + 4
            v = read_mask[i]
            if v > 0:
                #print(col_ind)
                tpid = record.pids[col_ind]
                offset = record.offset

                # second hop needed
                if record.mask[i] > 0:
                    #print("HOP")
                    tpid = latest_rec.pids[col_ind]
                    offset = latest_rec.offset
                #print("Reading", tpid, offset)
                r = self.columns[col_ind].read(tpid, offset)
                res.append(r)
        
        rt = QueryResult(res)
        return rt
    
    
    #After Retriving a LID for the record, then setting special val for the rids,
    # Get ready for the merge process

    def key_to_baseRid(self,key):
        if not key in self.fake_index:
            return None
        return self.fake_index[key]

    # def set_delete_flag(self, key):
    #     delete_lid = self.key_lid[key]
    #     delete_rid = self.lid_rid[delete_lid]
    #     self.deleted_base_rid.append(delete_rid)
    #     del self.key_lid[key]
    #     #del self.lid_rid[delete_lid]
    
    def __merge(self):
        pass

    def update_page_directory(self):
        pass
 
