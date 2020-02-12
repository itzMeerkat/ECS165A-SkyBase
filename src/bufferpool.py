from .config import *
from .page import Page

class Bufferpool:

    def __init__(self):
        #self.buffer_pages=Node(-1)
        self.cache={}
        self.head=DLinkedNode()
        self.tail=DLinkedNode()
        self.head.next=self.tail 
        self.tail.prev=self.head

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
        if(res.pirLcount!=0):
            res=res.prev
        return res
    #function operate Double Linked List ends

    
    def add_page(self,pid):  #put a page into bufferpool
        if(not self.has_capacity()):
            self.release_one_page()
        node=DLinkedNode()
        node.key=pid
        self.cache[pid]=node
        self._add_to_head(node)

        self.num_pages+=1
        #self.page_request(page)

    def release_one_page(self):  
        res=self._pop_tail()
        self._remove_node(res)
        if(res.dirty==True):
            #write into disk
            res.dirty=False
        self.num_pages-=1
        pass

    def get(self,pid):        #get a page from bufferpool
        node = self.cache.get(pid)
        if not node:
            return -1
        self._move_to_head(node)
        return node.key

    def pin(self,pid):
        node = self.cache.get(pid)
        if not node:
            #error
            pass
        node.pirLcount += 1

    def unpin(self,pid):
        node = self.cache.get(pid)
        if not node:
            #error
            pass
        node.pirLcount -= 1

    def has_capacity(self):
        return self.num_pages < self.capacity


#use pid as key of DLinkedNode
class DLinkedNode:
    def __init__(self):
        self.key=0
        self.pirLcount=0   
        self.dirty=False
        self.next=None 
        self.prev=None





