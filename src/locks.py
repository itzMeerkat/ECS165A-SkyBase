from threading import Lock,RLock

class RecordLocks:
    def __init__(self):
        self.locks = {}
        self.NEW_LOCK_LOCK = Lock()
    
    def acquire(self, rid):
        if rid in self.locks:
            r = self.locks[rid].acquire(blocking=False)
            return r

        self.NEW_LOCK_LOCK.acquire()
        self.locks[rid] = RLock()
        self.locks[rid].acquire()
        self.NEW_LOCK_LOCK.release()
        return True

    def release(self, rid):
        self.locks[rid].release()
