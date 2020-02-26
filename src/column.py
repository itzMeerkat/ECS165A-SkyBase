from .config import *
from time import time
from .page import Page
from .bufferpool import *

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
        self.bufferpool = None

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
    def __init__(self,bufferpool,column_index):
        self.col_index = column_index
        #self.base_pages = {}
        #self.tail_pages = {} # pid: Page()
        self.len_base = 0
        self.len_tail = {} #number of tail page
        self.bufferpool = bufferpool

    def _append_tail_page(self, base_group):
        pid = self.build_pid(base_group, self.len_tail[base_group] + 1)
        self.bufferpool.new_page(pid,TAIL_PAGE)
        #self.tail_pages[pid] = Page()
        self.len_tail[base_group] += 1
        #print("Append tail:", pid)
        return pid
    
    def _append_base_page(self):
        pid = self.build_pid(-1, self.len_base)
        self.bufferpool.new_page(pid,BASE_PAGE)
        #self.base_pages[pid] = Page(is_tail=False)
        self.len_base += 1

        base_group = self.base_pid_to_group(pid)
        if not base_group in self.len_tail:
            self.len_tail[base_group] = 0
        #print("Append base:", pid)
        return pid


    def _write_tail(self, val, base_group, rid):
        tar_pid = self.build_pid(base_group, self.len_tail[base_group])
        if(self.len_tail[base_group] <= 0):
            tar_pid = self._append_tail_page(base_group)

        node = self.bufferpool.access(tar_pid)
        if(node.page.has_capacity() is False):
            tar_pid = self._append_tail_page(base_group)
            self.bufferpool.access_finish(node,0)    #unpin the page that is full
            node = self.bufferpool.access(tar_pid)
        """
        if (self.len_tail[base_group] <= 0) or (self.tail_pages[tar_pid].has_capacity() is False):
            tar_pid = self._append_tail_page(base_group)
        """
        
        offset = node.page.write(val,rid)
        self.bufferpool.access_finish(node,1)
        #offset = self.tail_pages[tar_pid].write(val)
        
        return tar_pid, offset

    def read(self, pid, offset):
        node = self.bufferpool.access(pid)
        val = node.page.read(offset)
        self.bufferpool.access_finish(node,0)

        return val
        
        """
        if self.is_tail_pid(pid):
            #node = self.bufferpool.access(pid)
            #print(pid)
            return self.tail_pages[pid].read(offset)
        else:
            #print(pid)
            return self.base_pages[pid].read(offset)
        """
        

    def _write_base(self, val,rid):
        tar_pid = self.build_pid(-1, self.len_base - 1)
        if(self.len_base-1<0):
            tar_pid = self._append_base_page()

        node = self.bufferpool.access(tar_pid)
        if(node.page.has_capacity() is False):
            tar_pid = self._append_base_page()
            self.bufferpool.access_finish(node,1)
            node = self.bufferpool.access(tar_pid)

        """
        if (self.len_base - 1 < 0) or (self.base_pages[tar_pid].has_capacity() is False):  
            tar_pid = self._append_base_page()
        """
        
        
        
        offset = node.page.write(val,rid)
        self.bufferpool.access_finish(node,1)
        #offset = self.base_pages[tar_pid].write(val)
        #tar_pid |= (self.col_index << 56)
        return tar_pid, offset

    def write(self, val, dest, base_pid, rid):
        if dest == TO_BASE_PAGE:
            return self._write_base(val,rid)
        elif dest == TO_TAIL_PAGE:
            return self._write_tail(val, self.base_pid_to_group(base_pid),rid)
    
    def inplace_update(self, pid, offset, val):
        node = self.bufferpool.access(pid)
        node.page.inplace_update(offset,val)
        self.bufferpool.access_finish(node,1)

        """
        if self.is_tail_pid(pid):
            self.tail_pages[pid].inplace_update(offset, val)
        else:
            self.base_pages[pid].inplace_update(offset, val)
        """


    # Max 8bitscolumns, 28bits base groupsper table; and 28bits tail pages per base page group
    def build_pid(self, base_group, index):
        return ((self.col_index << 56) | ((base_group + 1) << 28) | (index & ((1<<28) -1)))
    
    def is_tail_pid(self, pid):
        tpid = (self.col_index << 56) ^ pid
        if tpid > ((1 << 28) - 1):
            return True
        return False


    def base_pid_to_group(self, base_pid):
        tbase_pid = (self.col_index << 56) ^ base_pid
        return int(tbase_pid / PARTITION_SIZE) + 1