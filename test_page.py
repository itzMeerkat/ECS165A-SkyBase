from src.page import Page

p = Page()
print(p.has_capacity())
p.write(255)
p.write(1024)
p.write(1023)
d, c = p.dump()
print(c)
print(d)
