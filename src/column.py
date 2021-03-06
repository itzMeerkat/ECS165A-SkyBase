from .config import *
from time import time
from .page import Page
from .bufferpool import *
from .bits import Bits

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
        self.pids = []
        self.offset = None

    def meta(self):
        return [self.indirection, self.rid, self.timestamp, self.mask.bits]


    def toJSON(self):
        return {
            'meta': self.meta(),
            'pids': self.pids,
            'offset': self.offset
        }
    
    def fromJSON(self,dic):
        self.pids = dic['pids']
        self.offset = dic['offset']
        m = dic['meta']
        self.indirection = int(m[0])
        self.rid = int(m[1])
        self.timestamp = int(m[2])
        self.mask = Bits("")
        self.mask.bits = int(m[3])

    def __str__(self):
        return str(self.toJSON())

class Column:
    def __init__(self,bufferpool,column_index):
        self.col_index = column_index
        # self.base_pages = {}
        # self.tail_pages = {} # pid: Page()
        self.len_base = 0
        self.len_tail = {} #number of tail page
        self.bufferpool = bufferpool

    def _append_tail_page(self, base_group):
        pid = self.build_pid(base_group, self.len_tail[base_group] + 1)

        #may lock bufferpool here

        self.bufferpool.new_page(pid)
        #self.tail_pages[pid] = Page()
        self.len_tail[base_group] += 1
        #print("Append tail:", pid)
        return pid
    
    def _append_base_page(self):
        pid = self.build_pid(-1, self.len_base)

        #may lock bufferpool here

        self.bufferpool.new_page(pid)
        #self.base_pages[pid] = Page()
        self.len_base += 1

        base_group = self.base_pid_to_group(pid)
        if not base_group in self.len_tail:
            self.len_tail[base_group] = 0
        #print("Append base:", pid)
        return pid


    def _write_tail(self, val, base_group):
        tar_pid = self.build_pid(base_group, self.len_tail[base_group])
        if(self.len_tail[base_group] <= 0):
            tar_pid = self._append_tail_page(base_group)

        #may lock bufferpool here
        node = self.bufferpool.access(tar_pid)
        if(node.page.has_capacity() is False):
            tar_pid = self._append_tail_page(base_group)
            self.bufferpool.access_finish(node,0)    #unpin the page that is full
            node = self.bufferpool.access(tar_pid)
        
        
        offset = node.page.write(val)
        self.bufferpool.access_finish(node,1)
        
        return tar_pid, offset

    def read(self, pid, offset):
        #may lock bufferpool here
        node = self.bufferpool.access(pid)
        val = node.page.read(offset)
        self.bufferpool.access_finish(node, 0)

        return val

    def _write_base(self, val):
        tar_pid = self.build_pid(-1, self.len_base - 1)
        if self.len_base - 1 < 0:
            tar_pid = self._append_base_page()

        #we may lock bufferpool here
        node = self.bufferpool.access(tar_pid)
        if(node.page.has_capacity() is False):
            tar_pid = self._append_base_page()
            self.bufferpool.access_finish(node,1)
            node = self.bufferpool.access(tar_pid)
        
        offset = node.page.write(val)
        self.bufferpool.access_finish(node,1)

        return tar_pid, offset

    def write(self, val, dest, base_pid):
        if dest == TO_BASE_PAGE:
            return self._write_base(val)
        elif dest == TO_TAIL_PAGE:
            return self._write_tail(val, self.base_pid_to_group(base_pid))
    
    def inplace_update(self, pid, offset, val):
        node = self.bufferpool.access(pid)
        node.page.inplace_update(offset,val)
        self.bufferpool.access_finish(node,1)


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