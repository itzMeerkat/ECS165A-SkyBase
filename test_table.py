from src.table import Table
from src.bits import Bits
table = Table("test", 5, 0)

table.put(1,None,0,Bits('11111'),[1,2,3,4,5])
table.put(2, 1, 0, Bits('11011'), [14, 15, None, 12, 13])
table.put(3, 1, 0, Bits('10100'), [99, None, 199, None, None])
r = table.get(1, Bits('11111'))
print(r)
