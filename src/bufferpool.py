from .config import *
from .page import Page

class Bufferpool:

    def __init__(self, bp_handler, tp_handler):
        self.cache={}
        self.dirty_pages=set()     
        self.head=DLinkedNode(-1)
        self.tail=DLinkedNode(-2)
        self.head.next=self.tail 
        self.tail.prev=self.head
        self.bp_handler = bp_handler
        self.tp_handler = tp_handler
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

    def get(self,pid,offset):        #get a page from bufferpool
        node = self.cache.get(pid)
        if not node:
            self.read_from_disk(pid,offset)
            return FAIL
        else:
            self._move_to_head(node)
            return node.key

    def pin(self,pid):
        node = self.cache.get(pid)
        #if not node:
            #error
        node.pirLcount += 1

    def unpin(self,pid):
        node = self.cache.get(pid)
        #if not node:
            #error
        node.pirLcount -= 1

    def has_capacity(self):
        return self.num_pages < self.capacity

    def write_back_all_dirty_page(self):
        for page_id in self.dirty_pages:
            self.dirty_pages.remove(page_id)
            #write_back_to_page

    def flush_to_disk(self,pid):
        pass

    def read_from_disk(self,pid,offset):
        pass


#use pid as key of DLinkedNode
class DLinkedNode:
    def __init__(self,key):
        self.key=key
        self.data=None #data supposed to be page. will be fixed future
        self.pirLcount=0   
        self.next=None 
        self.prev=None





