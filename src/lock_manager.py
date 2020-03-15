import threading



class LockTable:

    def __init__(self):
        self.lock_table={}
        self.lock = threading.Lock()

    def acquire_read(self,rid,tid):
        #print("Try Lock S", rid, tid)
        self.lock.acquire()
        print("Lock S",rid,tid)
        if rid in self.lock_table:
            counter=self.lock_table[rid]
            if(counter.writers==0 or counter.writer_transaction==tid):
                counter.reader_transactions.add(tid)
                counter.readers+=1
                self.lock.release()
                return True
            else:
                self.lock.release()
                return False
        else:
            counter=Counter()
            self.lock_table[rid]=counter
            counter.reader_transactions.add(tid)
            counter.readers+=1
            self.lock.release()
            return True
        

    def release_read(self,rid,tid):
        self.lock.acquire()
        print("Release S", rid, tid)
        counter=self.lock_table[rid]
        counter.reader_transactions.remove(tid)
        counter.readers-=1
        self.lock.release()

    def acquire_write(self,rid,tid):
        self.lock.acquire()
        print("Lock X", rid, tid)
        if rid in self.lock_table:
            counter=self.lock_table[rid]
            if(counter.readers==0 and counter.writers==0):
                counter.writer_transaction=tid
                counter.writers+=1
                self.lock.release()
                return True
            elif(counter.readers==1 and tid in counter.reader_transactions):
                counter.reader_transactions.remove(tid)
                counter.readers=0
                counter.writer_transaction=tid
                counter.writers+=1
                self.lock.release()
                return True
            elif(counter.writer_transaction==tid):
                self.lock.release()
                return True
            else:
                self.lock.release()
                return False
        else:
            counter=Counter()
            self.lock_table[rid]=counter
            counter.writer_transaction=tid
            counter.writers+=1
            self.lock.release()
            return True
        

    def release_write(self,rid,tid):
        self.lock.acquire()
        print("Release X", rid, tid)
        counter=self.lock_table[rid]
        counter.writer_transaction=None
        counter.writers-=1
        self.lock.release()

    def _upgrade(self,rid,tid):
        self.lock.acquire()
        print("Upgrade", rid, tid)
        counter=self.lock_table[rid]
        if(counter.writers>0):
            self.lock.release()
            return False
        else:
            counter.reader_transactions.remove(tid)
            counter.readers=0
            counter.writer_transaction=tid
            counter.writers+=1
            self.lock.release()
            return True
        

class Counter:

    def __init__(self):
        self.readers=0
        self.writers=0
        self.reader_transactions=set()
        self.writer_transaction=None
