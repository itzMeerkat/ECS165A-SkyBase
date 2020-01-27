from src.page import *
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:
    '''
    page_ids and offsets contains number of cols elements
    '''
    def __init__(self, rid, key, col_mask, page_ids, offsets):
        self.key = key
        
        self.rid = rid
        self.timestamp = int(time.time() * 1000) #milli sec timestamp
        self.col_mask = col_mask
        self.indirection = None
    

    def get_indirection(self):
        return self.indirection
        

    def set_indirection(self, val):
        self.indirection = val

class Column:
    def __init__(self):
        self.pages = [Page()]
        self.num_base_pages = 1
        self.num_pages = 1

    def _append_tail_page(self):
        self.pages.append(Page())
        self.num_pages += 1
        return self.num_pages
    
    def _append_base_page(self):
        self.pages.insert(self.num_base_pages, Page())
        self.num_base_pages += 1
        self.num_pages += 1
        return self.num_base_pages


    def _write(self, val):
        tar_pid = len(self.pages)
        if not self.pages[tar_pid].has_capacity():
            tar_pid = self._append_tail_page()
        
        offset = self.pages[tar_pid].write(val)
        return tar_pid, offset

    def _read(self, rid):
        pass

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

        self.columns = [Column()] * self.num_columns

        # Only goes up
        self.next_rid = 0

        # {rid: Record obj}
        self.page_directory = {}

    """
    mask: string for debuging, will switch to bitmap
    """
    def update(self, rid, key, mask, cols):
        # For each 1 in mask, call update func in that column
        k = 0
        pids = []
        offsets = []
        for i,v in enumerate(mask):
            if v == '1':
                pid, offset = self.columns[i]._write(cols[k])

                pids.append(pid)
                offsets.append(offset)

                k += 1
        
        rec = Record(self._get_rid(), key, mask, pids, offsets)
        self.page_directory[rid] = rec
        
                
    def _get_rid(self):
        self.next_rid += 1
        return self.next_rid

    def __merge(self):
        pass
 
