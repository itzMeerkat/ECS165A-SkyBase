from src.query import Query
from src.table import Table
table = Table("test", 5, 0)

query = Query(table)
query.insert(1, 90, 0, 0, 0)
query.insert(2, 91, 0, 0, 0)
query.insert(3, 92, 0, 0, 0)

# r = table.get(1, Bits('11001'))
# print(r)
