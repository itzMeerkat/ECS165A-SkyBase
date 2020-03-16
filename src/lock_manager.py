from threading import RLock, Lock



class LockTable:

    def __init__(self):
        self.lock_table={}
        self.lock = Lock()

    def acquire_read(self,rid,tid):
        #print("Try Lock S", rid, tid)
        self.lock.acquire()
        #print("Lock S",rid,tid)
        if rid in self.lock_table:
            counter=self.lock_table[rid]
            if(counter.writer_transaction is None or counter.writer_transaction == tid):
                counter.reader_mod_lock.acquire()
                counter.reader_transactions.add(tid)
                counter.readers += 1
                counter.reader_mod_lock.release()
                self.lock.release()
                return True
            else:
                self.lock.release()
                return False
        else:
            counter=Counter()
            self.lock_table[rid]=counter
            counter.reader_mod_lock.acquire()
            counter.reader_transactions.add(tid)
            counter.readers += 1
            counter.reader_mod_lock.release()
            self.lock.release()
            return True
        

    def release_read(self,rid,tid):
        self.lock.acquire()
        #print("Release S", rid, tid)
        counter=self.lock_table[rid]
        counter.reader_transactions.remove(tid)
        counter.reader_mod_lock.acquire()
        counter.readers -= 1
        counter.reader_mod_lock.release()
        self.lock.release()

    def acquire_write(self,rid,tid):
        self.lock.acquire()
        #print("Lock X", rid, tid)
        if rid in self.lock_table:
            counter=self.lock_table[rid]

            r = counter.wlock.acquire(blocking=False)
            if(counter.readers==0 and r is True):
                counter.writer_transaction=tid
                self.lock.release()
                return True
            elif(counter.readers==1 and tid in counter.reader_transactions):
                counter.reader_transactions.remove(tid)

                counter.reader_mod_lock.acquire()
                counter.readers-=1
                counter.reader_mod_lock.release()

                counter.writer_transaction=tid
                self.lock.release()
                return True
            elif(counter.writer_transaction==tid):
                self.lock.release()
                return True
            else:
                self.lock.release()
                return False
        else:
            counter = Counter()
            self.lock_table[rid]=counter
            counter.writer_transaction=tid
            r = counter.wlock.acquire(blocking=False)
            self.lock.release()
            return r
        

    def release_write(self,rid,tid):
        self.lock.acquire()
        #print("Release X", rid, tid)
        counter=self.lock_table[rid]
        counter.writer_transaction=None
        counter.wlock.release()
        self.lock.release()

    # def _upgrade(self,rid,tid):
    #     self.lock.acquire()
    #     print("Upgrade", rid, tid)
    #     counter=self.lock_table[rid]
    #     if(counter.writers>0):
    #         self.lock.release()
    #         return False
    #     else:
    #         counter.reader_transactions.remove(tid)
    #         counter.readers = 0
    #         counter.writer_transaction=tid
    #         r = counter.wlock.acquire(blocking=False)
    #         self.lock.release()
    #         return r
        

class Counter:

    def __init__(self):
        self.readers=0
        self.reader_mod_lock = Lock()
        self.wlock = RLock()
        self.reader_transactions=set()
        self.writer_transaction=None
