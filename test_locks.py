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


lock_table = RecordLocks()

#below is multi-thread test
class Lock1:

    def __init__(self):
        self.name = "thread1"

    def run(self):
        print("lock1 acquire1",lock_table.acquire(1))
        print("lock1 acquire2",lock_table.acquire(2))

class Lock2:

    def __init__(self):
        self.name = "thread2"

    def run(self):
        print("lock2 acquire3",lock_table.acquire(3))
        print("lock2 acquire1",lock_table.acquire(1))
        print("lock2 acquire2",lock_table.acquire(2))
        
class Lock3:

    def __init__(self):
        self.name = "thread3"

    def run(self):
        #print("lock2 acquire3",self.lock_table.acquire(3,2))
        print("lock3 acquire1",lock_table.acquire(3))
        print("lock3 acquire2",lock_table.acquire(2))


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





