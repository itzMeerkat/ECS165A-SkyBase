from src.bufferpool import Bufferpool
bp=Bufferpool()
bp.add_page(1)
bp.add_page(2)
bp.pin(2)
bp.pin(1)
bp.unpin(1)
print(bp.add_page(3))
print(bp.get(3))
