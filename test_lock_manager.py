from src.lock_manager import *

lock_table=LockTable()

class Lock1:

    def __init__(self):
        self.name = "thread1"

    def run(self):
        print("lock1 acquire1",lock_table.acquire_write(1))
        print("lock1 acquire2",lock_table.acquire_write(2))
        print("lock1 release1",lock_table.release_write(1))
        #print("lock1 release2",lock_table.release(1))

class Lock2:

    def __init__(self):
        self.name = "thread2"

    def run(self):
        print("lock2 acquire3",lock_table.acquire_write(3))
        print("lock2 acquire1",lock_table.acquire_write(1))
        print("lock2 acquire2",lock_table.acquire_write(2))
        
class Lock3:

    def __init__(self):
        self.name = "thread3"

    def run(self):
        #print("lock2 acquire3",self.lock_table.acquire(1))
        print("lock3 acquire3",lock_table.acquire_write(3))
        print("lock3 acquire2",lock_table.acquire_write(2))
        print("lock3 acquire4",lock_table.acquire_write(4))


"""
lock0=RecordLocks()
print("lock0 acquire1",lock0.acquire(1))
print("lock0 acquire1",lock0.acquire(1))
"""


threads=[]
lock1=Lock1()
lock2=Lock2()
lock3=Lock3()

threads.append(threading.Thread(target=lock1.run,args=()))
threads.append(threading.Thread(target=lock2.run,args=()))
threads.append(threading.Thread(target=lock3.run,args=()))


for thread in threads:
    thread.start()