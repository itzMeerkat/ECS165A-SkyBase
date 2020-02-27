from .config import *

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)
        #self.bids = bytearray(PAGE_SIZE)
        self.lineage = 0 # remeber to put this on the first spot in the page when flushing

    def has_capacity(self):
        if MAX_RECORDS > self.num_records:
            return True
        return False

    '''
    Each write will write a 64-bit long data into page
    TODO: benchmark of byte conversion
    '''
    def write(self, value):
        #print(self.num_records)
        #self.lineage += 1
        self.num_records += 1
        l = self.num_records * COL_SIZE
        r = l + COL_SIZE
        self.data[l:r] = value.to_bytes(8, 'big')

        #self.bids[l:r] = bid.to_bytes(8, 'big')
        return self.num_records

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

    def to_disk(self):
        self.data[0:COL_SIZE] = self.lineage.to_bytes(8, 'big')
        return self.data

    def from_disk(self, d):
        self.data = d
        self.lineage = int.from_bytes(self.data[0:COL_SIZE], 'big')


    
