import os
from .config import *
from .page import Page

class Bufferpool:

    def __init__(self, file_handler):
        self.cache={}
        #self.dirty_pages=set()     
        self.head=DLinkedNode(-1)
        self.tail=DLinkedNode(-2)
        self.head.next=self.tail 
        self.tail.prev=self.head
        self.file_handler = file_handler
        self.num_pages=0
        self.capacity = BUFFERPOOL_SIZE

    #function operate Double Linked List directly
    #---------------------------------------------------------------------------------
    def _add_to_head(self,node):
        node.prev=self.head
        node.next=self.head.next
        self.head.next.prev=node
        self.head.next=node

    def _remove_node(self,node):
        prev=node.prev
        next=node.next
        prev.next=next
        next.prev=prev

    def _move_to_head(self,node):
        self._remove_node(node)
        self._add_to_head(node)

    def _pop_tail(self):
        res=self.tail.prev
        while(res.pirLcount!=0):
            res=res.prev
        return res
    #---------------------------------------------------------------------------------
    #function operate Double Linked List ends

    def new_page(self,pid):
        print("New page", pid)
        if(self.has_capacity() is False):
            sign = self._release_one_page()
            if(sign == FAIL):
                print("Release failed")
        node = DLinkedNode(pid)
        self.cache[pid]=node
        self._add_to_head(node)
        self.num_pages+=1

    def _release_one_page(self):  
        res=self._pop_tail()
        if(res.key==-1):
            return FAIL    #no page could be released at that time,maybe abort transaction

        self._remove_node(res)
        del self.cache[res.key]
        if(res.dirty is True):
            self.flush_to_disk(res.key)
            #self.dirty_pages.remove(res.key)
        self.num_pages-=1
        return SUCCESS

    def access(self,pid):        #get a page from bufferpool
        node = self.cache.get(pid)
        if not node:
            pass
            signal = self.read_from_disk(pid)
            if(signal == FAIL):
                pass
            node = self.cache.get(pid)
            node.pirLcount+=1 #pin this page
            return node
        else:
            node.pirLcount+=1   #pin this page
            self._move_to_head(node)
            return node

    """
    when read and write finish, should unpin page. If it's write, it should make this page dirty
    """
    def access_finish(self,node,signal): #signal == 1: write; signal == 0: read
        if(signal == 1):
            node.dirty = True
            #self.dirty_pages.add(node.key)
        node.pirLcount -= 1  #unpin this page

    def has_capacity(self):
        return self.num_pages < self.capacity

    
    def write_back_all_dirty_page(self):
        curr = self.head
        while(curr.next!=None):
            if(curr.dirty==False):
                self.flush_to_disk(curr.key)
            curr=curr.next
        """
        for page_id in self.dirty_pages:
            self.dirty_pages.remove(page_id)
            self.flush_to_disk(page_id)
        """

    def flush_to_disk(self,pid):
        data_array = self.cache[pid].data
        fh_index = 0
        #check if tail or base pid
        if pid > ((1 << 32) - 1):
            fh_index = 1
        #seek the meta data and data file to the beginning of the file
        meta_handler = self.file_handler[fh_index] #meta data
        f_handler = self.file_handler[fh_index+2] #actual file
        meta_handler.seek(0)
        f_handler.seek(0)

        #first need to look for the pid in the metadata file of the given base page or tail page file
        search_pid = "(" + str(pid) + ","
        begin = meta_handler.read().find(search_pid)
        if begin == -1:
            f_handler.seek(0,2)
            meta_handler.write(pid + str(f_handler.tell()) + ")")
        else:
            meta_handler.seek(begin)
            end = meta_handler.read().find(")")
            meta_handler.seek(begin)
            file_index = meta_handler.read(end).split(",")[1]
            f_handler.seek(int(file_index))
        f_handler.write("".join(map(str, data_array)))

     
    def read_from_disk(self,pid):
        #Create a new node and update with cache
        bt = 0
        if pid > ((1 << 32 -1)):
            bt = 1
        meta_handler = self.file_handler[bt] #Meta data file handler
        f_handler = self.file_handler[bt+2] #Base/tail Data file handler 
        meta_handler.seek(0)
        f_handler.seek(0)
        search_pid = "(" + str(pid) + ","
        begin = meta_handler.read().find(search_pid) #Look for pid in meta
        if begin == FAIL:
            return FAIL
        meta_handler.seek(begin)
        end = meta_handler.read().find(")")
        meta_handler.seek(begin)
        file_index = meta_handler.read(end).split(",")[1]
        f_handler.seek(int(file_index))
        data_array = f_handler.read(4096)
        return self.add_page_from_disk(pid,data_array)
    
    def add_page_from_disk(self,pid,data):
        # self.cache[pid] = data
        if(self.has_capacity() is False):
            sign=self._release_one_page()
            if(sign == -1):
                return FAIL  
        node = DLinkedNode(pid)
        self.cache[pid] = node
        self._add_to_head(node)
        self.num_pages+=1
        return SUCCESS
        #return node

"""
use pid as key of DLinkedNode.
operate data in page directly
"""
class DLinkedNode():
    def __init__(self,key,page=None):
        self.key=key
        if page is None:
            self.page = Page()
        else:
            self.page=page
        self.dirty = False
        self.pirLcount=0  
        self.next=None
        self.prev=None

        """
        self.num_records = 0
        self.free_index = [i for i in range(511, 0, -1)]
        self.MAX_RECORDS = PAGE_SIZE / COL_SIZE - 1
        self.lineage = 0 # remeber to put this on the first spot in the page when flushing
        """

    """
    def has_capacity(self):
        if self.MAX_RECORDS > self.num_records:
            return True
        return False

    '''
    Each write will write a 64-bit long data into page
    TODO: benchmark of byte conversion
    '''
    def write(self, value):
        #print(self.num_records)
        self.lineage += 1
        insert_index = self.free_index.pop()
        l = insert_index * COL_SIZE
        r = l + COL_SIZE
        self.data[l:r] = value.to_bytes(8, 'big')
        self.num_records += 1
        return insert_index

    def read(self, offset):
        l = offset * COL_SIZE
        r = l + COL_SIZE
        value = int.from_bytes(self.data[l:r], 'big')
        return value


    def remove(self, index):
        self.free_index.append(index)
        self.num_records -= 1
        return

    def inplace_update(self, offset, val):
        l = offset * COL_SIZE
        r = l + COL_SIZE
        self.data[l:r] = val.to_bytes(8, 'big')
    """





