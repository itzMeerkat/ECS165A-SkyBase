from src.bufferpool import Bufferpool
bp=Bufferpool()
bp.add_page(1)
bp.release_one_page()
#print(bp._pop_tail().key)
print(bp.get(1))
