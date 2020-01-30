from src.table import Table

table = Table("test", 5, 0)

table.put(1,None,0,'11111',[1,2,3,4,5])
r = table.get(1,'11111')
print(r)