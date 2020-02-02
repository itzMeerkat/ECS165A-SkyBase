from src.query import Query
from src.table import Table
from src.bits import Bits
table = Table("test", 5, 0)

query = Query(table)
record1 = [1, 90, 0, 0, 0]
record2 = [2, 91, 0, 0, 0]
record3 = [3, 92, 0, 0, 0]
query.insert(record1)
query.insert(record2)
query.insert(record3)


r1 = table.get(1, Bits('11111'))
r2 = table.get(2, Bits('11111'))
r3 = table.get(3, Bits('11111'))
print(r1)
print(r2)
print(r3)

query.delete(record1[0])
print("After deletion record1's LID: ", table.keys[1])