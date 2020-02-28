from src.bufferpool import Bufferpool
"""
bp=Bufferpool()
bp.add_page(1)
bp.add_page(2)
bp.pin(2)
bp.pin(1)
bp.unpin(1)
print(bp.add_page(3))
print(bp.get(3))
"""

bp = Bufferpool(0)
bp.new_page(1)
bp.new_page(2)
bp.access(1).dirty = True
bp.write_back_all_dirty_page()


