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
        self.meta_data = None
        self.locations = [] # contain tuples (pid, offset)

    def get_indirection(self):
        return self.indirection
        

    def set_indirection(self, val):
        if not val is None:
            self.indirection = val
        else:
            self.indirection = 0


    def meta(self):
        if self.meta_data is None:
            self.meta_data = [self.indirection, self.rid, self.timestamp, self.mask.bits]
        return self.meta_data

"""
TODO: free space reuse
"""
class Column:
    def __init__(self):
        self.pages = [[],[]]
        self.len_base = 0
        self.len_tail = 0

    def _append_tail_page(self):
        self.pages[1].append(Page())
        self.len_tail += 1
        return self.len_tail - 1
    
    def _append_base_page(self):
        self.pages[0].append(Page())
        self.len_base += 1
        return self.len_base - 1


    def _write_tail(self, val):
        tar_pid = self.len_tail - 1
        if (tar_pid < 0) or (self.pages[1][tar_pid].has_capacity() is False):
            tar_pid = self._append_tail_page()
        
        offset = self.pages[1][tar_pid].write(val)
        r_pid = (tar_pid << 1) | 1
        return r_pid, offset

    def read(self, pid, offset):
        bt = pid & 1
        pid >>= 1
        #print(bt, pid)
        return self.pages[bt][pid].read(offset)

    def _write_base(self, val):
        tar_pid = self.len_base - 1
        
        if (tar_pid < 0) or (self.pages[0][tar_pid].has_capacity() is False):

            tar_pid = self._append_base_page()
            
        #print(self.pages[0][tar_pid].has_capacity())
        offset = self.pages[0][tar_pid].write(val)
        r_pid = (tar_pid << 1)

        return r_pid, offset

    def write(self, val, dest):
        if dest == TO_BASE_PAGE:
            return self._write_base(val)
        elif dest == TO_TAIL_PAGE:
            return self._write_tail(val)
    
    def inplace_update(self, pid, offset, val):
        bt = pid & 1
        pid >>= 1
        self.pages[bt][pid].inplace_update(offset, val)

