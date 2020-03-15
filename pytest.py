from threading import Lock

l = Lock()
#l.acquire()
r = l.acquire(blocking=False)
print(r)