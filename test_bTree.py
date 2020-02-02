from src.btree import BTree

tree = BTree(10)
for i in range(0, 100):
    tree.insert(906659671 + i*2)
    # tree.search(906659671 + i*2)

tree.search(906659671 + 20*2)
# tree.search(906659671)

# tree.__str__()
 