from .config import *
from .page import Page

class Bufferpool:

    def __init__(self):
        self.buffer_pages=Node(-1)
        self.num_pages=0
        self.size = BUFFERPOOL_SIZE
        pass
    
    def add_page(self,page):
        if(not self.has_capacity()):
            self.release_one_page()
        
        self.page_request(page)

    def release_one_page(self):
        pass

    def page_request(self,page):
        pass

    def page_released(self,page):
        if(page.dirty is True):
            page.dirty = False
            pass
        pass

    def pin(self,page):
        page.pirLcount += 1
        pass

    def unpin(self,page):
        page.pirLcount -= 1
        if(page.pirLcount==0):
            pass
        pass

    def has_capacity(self):
        return self.num_pages < self.size

class Node:
    def __init__(self,pid,pnext=None):
        self.pid = pid
        self.next = pnext   

    def append(self):
        pass

    def delete(self):
        pass





