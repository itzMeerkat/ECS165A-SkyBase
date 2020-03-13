import threading

lock=threading.Lock()

class LockTable:

    def __init__(self):
        self.lock_table={}

    def acquire_read(self,rid,tid):
        lock.acquire(blocking=False)
        if rid in self.lock_table:
            counter=self.lock_table[rid]
            if(counter.writers==0 | counter.writer_transaction==tid):
                counter.reader_transactions.add(tid)
                counter.readers+=1
                return True
            else:
                return False
        else:
            counter=Counter()
            self.lock_table[rid]=counter
            counter.reader_transactions.add(tid)
            counter.readers+=1
            return True
        lock.release()

    def release_read(self,rid,tid):
        lock.acquire(blocking=False)
        counter=self.lock_table[rid]
        counter.reader_transactions.remove(tid)
        counter.readers-=1
        lock.release()

    def acquire_write(self,rid,tid):
        lock.acquire(blocking=False)
        if rid in self.lock_table:
            counter=self.lock_table[rid]
            if(counter.readers==0 and counter.writers==0):
                print("...................")
                print(counter.readers)
                print(counter.writers)
                counter.writer_transaction=tid
                counter.writers+=1
                return True
            elif(counter.readers==1 & tid in counter.reader_transactions):
                counter.reader_transactions.remove(tid)
                counter.readers=0
                counter.writer_transaction=tid
                counter.writers+=1
                return True
            elif(counter.writer_transaction==tid):
                return True
            else:
                return False
        else:
            counter=Counter()
            self.lock_table[rid]=counter
            counter.writer_transaction=tid
            counter.writers+=1
            return True
        lock.release()

    def release_write(self,rid,tid):
        lock.acquire(blocking=False)
        counter=self.lock_table[rid]
        counter.writer_transaction=None
        counter.writers-=1
        lock.release()

    def _upgrade(self,rid,tid):
        lock.acquire(blocking=False)
        counter=self.lock_table[rid]
        if(counter.writers>0):
            return False
        else:
            counter.reader_transactions.remove(tid)
            counter.readers=0
            counter.writer_transaction=tid
            counter.writers+=1
            return True
        lock.release()

class Counter:

    def __init__(self):
        self.readers=0
        self.writers=0
        self.reader_transactions=set()
        self.writer_transaction=None