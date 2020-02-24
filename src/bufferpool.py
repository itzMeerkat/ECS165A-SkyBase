import os
from .config import *
from .page import Page

class Bufferpool:

    def __init__(self, file_handler):
        self.cache={}
        self.dirty_pages=set()     
        self.head=DLinkedNode(-1)
        self.tail=DLinkedNode(-2)
        self.head.next=self.tail 
        self.tail.prev=self.head
        self.file_handler = file_handler
        self.num_pages=0
        self.capacity = BUFFERPOOL_SIZE

    #function operate Double Linked List directly
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
    #function operate Double Linked List ends

    def new_page(self,pid):
        node = DLinkedNode(pid,Page())
        self._move_to_head(node)
    
    def add_page(self,pid):   #put a page into bufferpool
        #node = self.cache.get(pid)
        #if(node!=-1)
        if(self.has_capacity() is False):
            sign=self._release_one_page()
            if(sign==-1):
                return FAIL  #cannot add this page to bufferpool now, maybe abort
        node=DLinkedNode(pid)
        node.key=pid
        self.cache[pid]=node
        self._add_to_head(node)
        self.num_pages+=1
        return SUCCESS

    def _release_one_page(self):  
        res=self._pop_tail()
        if(res.key==-1):
            return FAIL    #no page could be released at that time

        self._remove_node(res)
        del self.cache[res.key]
        if(res.key in self.dirty_pages):
            self.dirty_pages.remove(res.key)
            #flush to disk

        self.num_pages-=1
        return SUCCESS

    def get(self,pid):        #get a page from bufferpool
        node = self.cache.get(pid)
        if not node:
            return self.read_from_disk(pid)
        else:
            node.pirLcount+=1   #pin this page
            self._move_to_head(node)
            return node.data

    """
    when read and write finish, should unpin page. If it's write, it should make this page dirty
    """
    def access_finish(self,pid,signal): #signal == 1: write; signal == 0: read
        node = self.cache.get(pid)
        if(signal == 1):
            node.dirty = True
            self.dirty_pages.add(node.key)
        node.pirLcount -= 1  #unpin this page

    def has_capacity(self):
        return self.num_pages < self.capacity

    def write_back_all_dirty_page(self):
        for page_id in self.dirty_pages:
            self.dirty_pages.remove(page_id)
            self.flush_to_disk(page_id)

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
        if begin == -1:
            return FAIL
        meta_handler.seek(begin)
        end = meta_handler.read().find(")")
        meta_handler.seek(begin)
        file_index = meta_handler.read(end).split(",")[1]
        f_handler.seek(int(file_index))
        data_array = f_handler.read(4096)
        self.add_page_from_disk(pid,data_array)
    
    def add_page_from_disk(self,pid,data):
        # self.cache[pid] = data
        if(self.has_capacity() is False):
            sign=self._release_one_page()
            if(sign == -1):
                return FAIL  
        node = DLinkedNode(pid,data)
        node.key = pid
        self.cache[pid] = node
        self._add_to_head(node)
        self.num_pages+=1

#use pid as key of DLinkedNode
class DLinkedNode:
    def __init__(self,key,data):
        self.key=key
        self.data=data #data supposed to be page (data_array)
        self.dirty = False
        self.pirLcount=0  
        self.next=None
        self.prev=None





