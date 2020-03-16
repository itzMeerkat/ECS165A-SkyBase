from src.db import Database
from src.query import Query
from src.transaction import Transaction
from src.transaction_worker import TransactionWorker

from random import choice, randint, sample, seed

import threading


db = Database()
db.open('~/ECS165')
grades_table = db.create_table('Grades', 5, 0)

keys = []
records = {}
seed(3562901)
num_threads = 2

# Generate random records
for i in range(0, 10000):
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, 0, 0, 0, 0]
    q = Query(grades_table)
    q.insert(None, False, False, *records[key])

print("DB created")
# Create transactions and assign them to workers
transactions = []
transaction_workers = []
for i in range(num_threads):
    transaction_workers.append(TransactionWorker())


for i in range(100):
    key = choice(keys)
    record = records[key]
    c = record[1]
    transaction = Transaction()
    for i in range(5):
        c += 1
        q = Query(grades_table)
        transaction.add_query(q.select, key, 0, [1, 1, 1, 1, 1])
        q = Query(grades_table)
        transaction.add_query(q.update, key, *[None, c, None, None, None])
    transaction_workers[i % num_threads].add_transaction(transaction)

print("Transactions built")
threads = []
for transaction_worker in transaction_workers:
    threads.append(threading.Thread(target=transaction_worker.run, args=()))
print("Threads created")
for thread in threads:
    thread.start()

for thread in threads:
    thread.join()

num_committed_transactions = sum(t.result for t in transaction_workers)
print(num_committed_transactions)
db.close()
