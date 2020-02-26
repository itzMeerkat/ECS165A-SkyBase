from .config import *

class Page:

    def __init__(self, is_tail=True):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)
        if is_tail:
            self.bid = bytearray(PAGE_SIZE)
        self.free_index = 1
        self.MAX_RECORDS = PAGE_SIZE / COL_SIZE - 1
        self.lineage = 0 # remeber to put this on the first spot in the page when flushing

    def has_capacity(self):
        if self.MAX_RECORDS > self.num_records:
            return True
        return False

    '''
    Each write will write a 64-bit long data into page
    TODO: benchmark of byte conversion
    '''
    def write(self, value):
        #print(self.num_records)
        #self.lineage += 1
        insert_index = self.free_index
        self.free_index += 1
        l = insert_index * COL_SIZE
        r = l + COL_SIZE
        self.data[l:r] = value.to_bytes(8, 'big')
        self.num_records += 1
        return insert_index

    """
    TODO: Do we need to check if the offset valid?
    """
    def read(self, offset):
        l = offset * COL_SIZE
        r = l + COL_SIZE
        value = int.from_bytes(self.data[l:r], 'big')
        return value

    def inplace_update(self, offset, val):
        l = offset * COL_SIZE
        r = l + COL_SIZE
        self.data[l:r] = val.to_bytes(8, 'big')

    
