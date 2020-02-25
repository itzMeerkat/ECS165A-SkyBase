from .config import *
from time import time
from .page import Page
from .bufferpool import Bufferpool

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
        self.bufferpool=None

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
    def __init__(self,bufferpool):
        self.base_pages = []
        self.tail_pages = {} # pid: Page()
        self.len_base = 0
        self.len_tail = []
        #self.bufferpool = bufferpool

    def _append_tail_page(self, base_group):
        pid = self.build_pid(base_group, self.len_tail[base_group] + 1)
        #self.bufferpool.new_page(pid)
        self.tail_pages[pid] = Page()
        self.len_tail[base_group] += 1
        return pid
    
    def _append_base_page(self):
        self.base_pages.append(Page())
        #self.bufferpool.new_page(self.len_base)
        self.len_tail.append(0)
        self.len_base += 1
        return self.len_base - 1


    def _write_tail(self, val, base_group):
        tar_pid = self.build_pid(base_group, self.len_tail[base_group])

        """
        if (self.len_tail[base_group] <= 0) or (self.bufferpool.get(tar_pid).has_capacity() is False):#such get will not pin page, will be fix
            tar_pid = self._append_tail_page(base_group)
        """
        if (self.len_tail[base_group] <= 0) or (self.tail_pages[tar_pid].has_capacity() is False):
            tar_pid = self._append_tail_page(base_group)
        
        
        #offset = self.bufferpool.get(tar_pid).write(val)
        #self.bufferpool.access_finish(tar_pid,1)
        offset = self.tail_pages[tar_pid].write(val)
        
        return tar_pid, offset

    def read(self, pid, offset):
        if self.is_tail_pid(pid):
            return self.tail_pages[pid].read(offset)
        else:
            return self.base_pages[pid].read(offset)
        #bt = pid & 1
        #pid >>= 1
        #print(bt, pid)
        #return self.bufferpool.get(pid,offset)
        
        #return pid,bt
        """
        val = self.bufferpool.get(pid).read(offset)  
        self.bufferpool.access_finish(pid,0) 
        return val
        """

    def _write_base(self, val):
        tar_pid = self.len_base - 1
        

        #if (tar_pid < 0) or (self.bufferpool.get(tar_pid).has_capacity() is False):#such get will not pin page. will fix
        #    tar_pid = self._append_base_page()
        
        if (tar_pid < 0) or (self.base_pages[tar_pid].has_capacity() is False):
            tar_pid = self._append_base_page()
        

        #offset = self.bufferpool.get(tar_pid).write(val)
        #self.bufferpool.access_finish(tar_pid,1)
        offset = self.base_pages[tar_pid].write(val)

        return tar_pid, offset

    def write(self, val, dest, base_pid):
        if dest == TO_BASE_PAGE:
            return self._write_base(val)
        elif dest == TO_TAIL_PAGE:
            return self._write_tail(val, self.base_pid_to_group(base_pid))
    
    def inplace_update(self, pid, offset, val):
        # TODO: This should be taken over by buffer pool
        if self.is_tail_pid(pid):
            self.tail_pages[pid].inplace_update(offset, val)
        else:
            self.base_pages[pid].inplace_update(offset, val)
        #pass

    def build_pid(self, base_group, index):
        return ((base_group + 1) << 32) | (index & ((1 << 32) - 1))
    
    def is_tail_pid(self, pid):
        if pid > ((1 << 32) - 1):
            return True
        return False


    def base_pid_to_group(self, base_pid):
        return int(base_pid / PARTITION_SIZE) + 1