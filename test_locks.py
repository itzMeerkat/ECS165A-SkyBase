from src.locks import RecordLocks
import threading




#below is single-thread test
"""
lock_table = RecordLocks()
print(lock_table.acquire(1))
print(lock_table.acquire(1))
lock_table.acquire(2)
lock_table.release(2)
print(lock_table.acquire(2))
print(lock_table.acquire(1))
"""




#below is multi-thread test
class Lock1:

    def __init__(self):
        self.lock_table = RecordLocks()

    def run(self):
        print("lock1 acquire1",self.lock_table.acquire(1))
        print("lock1 acquire2",self.lock_table.acquire(2))

class Lock2:

    def __init__(self):
        self.lock_table = RecordLocks()

    def run(self):
        #print("lock2 acquire3",self.lock_table.acquire(3,2))
        print("lock2 acquire1",self.lock_table.acquire(1))
        print("lock2 acquire2",self.lock_table.acquire(2))


"""
lock0=RecordLocks()
print("lock0 acquire1",lock0.acquire(1))
print("lock0 acquire1",lock0.acquire(1))
"""


threads=[]
lock1=Lock1()
lock2=Lock2()

threads.append(threading.Thread(target=lock1.run,args=()))
threads.append(threading.Thread(target=lock2.run,args=()))


for thread in threads:
    thread.start()





