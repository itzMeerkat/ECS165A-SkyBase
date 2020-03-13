from src.waiting_two_phase_lock import *
import threading

lock_table=LockTableWait()

class Thread1:

    def __init__(self):
        self.name = "thread1"

    def run(self):
        print("lock1 read1",lock_table.acquire_read(1))
        print("lock1 acquire1",lock_table.acquire_write(1))
        #print("upgrade",lock_table.upgrade(1))
        #print("lock1 release write1",lock_table.release_write(1))
        print("lock1 acquire2",lock_table.acquire_write(2))

class Thread2:

    def __init__(self):
        self.name = "thread2"

    def run(self):
        print("lock2 acquire3",lock_table.acquire_write(3))
        print("lock2 acquire1",lock_table.acquire_write(1))
        print("lock2 acquire2",lock_table.acquire_write(2))
        print("lock2 read1",lock_table.acquire_read(1))
        print("lock2 read2",lock_table.acquire_read(2))
        
class Thread3:

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
thread1=Thread1()
thread2=Thread2()
thread3=Thread3()

threads.append(threading.Thread(target=thread1.run,args=()))
threads.append(threading.Thread(target=thread2.run,args=()))
threads.append(threading.Thread(target=thread3.run,args=()))


for thread in threads:
    thread.start()