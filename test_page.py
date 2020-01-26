from src.page import Page

p = Page()
print(p.has_capacity())
p.write(255)
p.write(1024)
p.write(1023)
print(p.read(0))
print(p.read(1))
print(p.read(2))
