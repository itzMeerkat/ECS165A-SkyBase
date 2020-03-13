import threading

class LockTableWait:

    def __init__(self):
        self.lock_table={}
        self.monitor=threading.Lock()

    def acquire_read(self,rid):
        self.monitor.acquire()
        if(rid in self.lock_table):
            lockCounter=self.lock_table[rid]
            lockCounter=LockCounter()
            while lockCounter.rwlock>0 or lockCounter.writers_waiting>0:
                lockCounter.reader_ok.wait(2)
            lockCounter.rwlock+=1
        else:
            pass
        self.monitor.release()

    def acquire_write(self,rid):
        self.monitor.acquire()
        if(rid in self.lock_table):
            lockCounter=self.lock_table[rid]
            while lockCounter.rwlock>0:
                lockCounter.writers_waiting+=1
                lockCounter.writers_ok.wait()
                lockCounter.writers_waiting-=1
        else:
            lockCounter=LockCounter()
        lockCounter.rwlock-=1
        self.monitor.release()

    def release(self,rid):
        self.monitor.acquire()
        pass
        self.monitor.release()

class LockCounter:

    def __init__(self):
        self.rwlock=0
        self.writers_waiting=0
        self.reader_ok=threading.Condition(threading.Lock())
        self.writers_ok=threading.Condition(threading.Lock())