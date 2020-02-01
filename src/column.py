from .config import *
from time import time
from .page import Page

class Record:
    '''
    page_ids and offsets contains number of cols elements
    '''
    def __init__(self, rid, key, col_mask):
        self.key = key
        
        self.indirection = 0
        self.rid = rid
        self.timestamp = int(time() * 1000) #milli sec timestamp
        self.mask = col_mask

        self.locations = [] # contain tuples (pid, offset)

    def get_indirection(self):
        return self.indirection
        

    def set_indirection(self, val):
        if not val is None:
            self.indirection = val
        else:
            self.indirection = 0


    def meta(self):
        return [self.indirection, self.rid, self.timestamp, self.mask.bits]

"""
TODO: free space reuse
"""
class Column:
    def __init__(self):
        self.pages = []
        self.num_base_pages = 0
        self.num_pages = 0

    def _append_tail_page(self):
        self.pages.append(Page())
        self.num_pages += 1
        return self.num_pages - 1
    
    def _append_base_page(self):
        self.pages.insert(self.num_base_pages, Page())
        self.num_base_pages += 1
        self.num_pages += 1
        return self.num_base_pages - 1


    def _write_tail(self, val):
        tar_pid = self.num_pages - 1
        if tar_pid < 0 or (not self.pages[tar_pid].has_capacity()):
            tar_pid = self._append_tail_page()
        
        offset = self.pages[tar_pid].write(val)
        return tar_pid, offset

    def read(self, pid, offset):
        return self.pages[pid].read(offset)

    def _write_base(self, val):
        tar_pid = self.num_base_pages - 1
        if tar_pid < 0 or (not self.pages[tar_pid].has_capacity()):
            tar_pid = self._append_base_page()
        
        offset = self.pages[tar_pid].write(val)
        return tar_pid, offset

    def write(self, val, dest):
        if dest == TO_BASE_PAGE:
            return self._write_base(val)
        elif dest == TO_TAIL_PAGE:
            return self._write_tail(val)
    
    def inplace_update(self, pid, offset, val):
        self.pages[pid].inplace_update(offset, val)

