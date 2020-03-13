import threading
from threading import Lock,RLock
class RecordLocks:
    def __init__(self):
        self.sharelocks = {}
        self.exclocks={}
        self.newlock = Lock()
    
    def acquire_share(self, rid):
        if rid in self.exclocks:
            return False
        elif rid in self.sharelocks:
            r = self.sharelocks[rid].acquire(blocking=False)
            return r
        else:
            self.newlock.acquire()
            self.sharelocks[rid] = RLock()
            self.sharelocks[rid].acquire()
            self.newlock.release()
            return True

    def acquire_exc(self,rid):
        if rid in self.exclocks:
            r = self.exclocks[rid].acquire(blocking=False)
            return r
        elif rid in self.sharelocks:
            self.upgrade(rid)
            return True
        else:
            self.newlock.acquire()
            self.exclocks[rid] = RLock()
            self.exclocks[rid].acquire()
            self.newlock.release()
            return True


    def release_share(self, rid):
        self.sharelocks[rid].release()

    def release_exc(self, rid):
        self.exclocks[rid].release()

    def upgrade(self, rid):
        self.sharelocks[rid].release()
        self.newlock.acquire()
        self.exclocks[rid] = RLock()
        self.exclocks[rid].acquire()
        self.newlock.release()

