import threading

lock=threading.Lock()

class LockTable:

    def __init__(self):
        self.lock_table={}

    def acquire_read(self,rid):
        lock.acquire(blocking=False)
        if rid in self.lock_table:
            counter=self.lock_table[rid]
            if(counter.writers==0):
                counter.readers+=1
                return True
            else:
                return False
        else:
            counter=Counter()
            self.lock_table[rid]=counter
            counter.readers+=1
            return True
        lock.release()

    def release_read(self,rid):
        lock.acquire(blocking=False)
        counter=self.lock_table[rid]
        counter.readers-=1
        lock.release()

    def acquire_write(self,rid):
        lock.acquire(blocking=False)
        if rid in self.lock_table:
            counter=self.lock_table[rid]
            if(counter.readers==0 | counter.writers==0):
                counter.writers+=1
                return True
            else:
                return False
        else:
            counter=Counter()
            self.lock_table[rid]=counter
            counter.writers+=1
            return True
        lock.release()

    def release_write(self,rid):
        lock.acquire(blocking=False)
        counter=self.lock_table[rid]
        counter.writers-=1
        lock.release()

    def upgrade(self,rid):
        lock.acquire(blocking=False)
        counter=self.lock_table[rid]
        if(counter.writers>0):
            return False
        else:
            counter.readers=0
            counter.writers+=1
            return True
        lock.release()

#----------------------------------------------------------#
class Counter:

    def __init__(self):
        self.readers=0
        self.writers=0