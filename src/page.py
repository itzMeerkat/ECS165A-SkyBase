from .config import *

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)
        self.MAX_RECORDS = PAGE_SIZE / COL_SIZE

    def has_capacity(self):
        if self.MAX_RECORDS > self.num_records:
            return True
        return False

    '''
    Each write will write a 64-bit long data into page
    '''
    def write(self, value):
        l = self.num_records * COL_SIZE
        r = l + COL_SIZE
        self.data[l:r] = value.to_bytes(8, 'big')
        self.num_records += 1

    def read(self, rid):
        l = rid * COL_SIZE
        r = l + COL_SIZE
        value = int.from_bytes(self.data[l:r], 'big')
        return value
