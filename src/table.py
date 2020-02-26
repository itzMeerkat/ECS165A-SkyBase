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
    def __init__(self, name, num_columns, key, file_handler, page_directory):
        self.name = name
        self.key = key
        self.num_columns = num_columns

        self.bufferpool = Bufferpool(file_handler)
        self.columns = [Column(self.bufferpool, i+1) for i in range(self.num_columns + META_COL_SIZE)]

        # {rid: Record obj}
        self.page_directory = page_directory
        self.rid = 1
        self.lid = 1
        self.lid_rid = {}
        self.key_lid = {}

        self.deleted_base_rid = []

    def get_next_rid(self):
        r = self.rid
        self.rid += 1
        return r

    def get_next_lid(self):
        r = self.lid
        self.lid += 1
        return r

    def _write_cols(self, mask, cols, dest, old_loc):
        #print("writing", mask.bits)
        locs = []
        l = mask.size
        for i in range(l):
            if mask[i] > 0:
                ol = None
                if not old_loc is None:
                    ol = old_loc[i][0]
                pid, offset = self.columns[i].write(cols[i], dest, ol)
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
        dest = TO_TAIL_PAGE
        new_lid = None
        old_loc = None
        if base_rid is None:
            dest = TO_BASE_PAGE
            new_lid = self.get_next_lid()
            self.key_lid[key] = new_lid
            self.lid_rid[new_lid] = rid
        else:
            base_record = self.page_directory[base_rid]
            old_loc = base_record.locations
            pre_rid = base_record.get_indirection()
            if pre_rid == 0:
                pre_rid = base_rid
            new_record.set_indirection(pre_rid)
            base_record.set_indirection(rid)

            # print("base, pre rid:", base_rid, pre_rid)

            # Inplace update base record indirection column
            base_ind_loc = base_record.locations[INDIRECTION_COLUMN]
            self.columns[INDIRECTION_COLUMN].inplace_update(base_ind_loc[0], base_ind_loc[1], base_record.get_indirection())

            base_schema_loc = base_record.locations[SCHEMA_ENCODING_COLUMN]

            base_record.mask.merge(write_mask)

            self.columns[SCHEMA_ENCODING_COLUMN].inplace_update(
                base_schema_loc[0], base_schema_loc[1], base_record.mask)


        # Combine meta cols and data cols
        meta_and_data = new_record.meta() + cols
        # b'1111' 
        write_mask.set_meta(15)
        #print("Writing mask",write_mask.bits)
        # print("Writing mask",write_mask.bits)
        locs = self._write_cols(write_mask, meta_and_data, dest, old_loc)

        # Merge old and new locations
        if dest == TO_TAIL_PAGE:
            for i in range(len(locs)):
                if locs[i] is None:
                    locs[i] = self.page_directory[pre_rid].locations[i]

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
            col_ind = i + 4
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
        
        rt = QueryResult(res)
        return rt
    
    
    #After Retriving a LID for the record, then setting special val for the rids,
    # Get ready for the merge process

    def key_to_baseRid(self,key):
        if not key in self.key_lid:
            return None
        lid = self.key_lid[key]
        return self.lid_rid[lid]

    def set_delete_flag(self, key):
        delete_lid = self.key_lid[key]
        delete_rid = self.lid_rid[delete_lid]
        self.deleted_base_rid.append(delete_rid)
        del self.key_lid[key]
        #del self.lid_rid[delete_lid]
    
    def __merge(self):
        pass

    def update_page_directory(self):
        pass
 
