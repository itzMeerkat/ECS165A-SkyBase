from .config import *

class Page:

    def __init__(self, is_tail=True):
        self.is_tail = is_tail
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)
        if is_tail:
            self.bids = bytearray(PAGE_SIZE)
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
    def write(self, value, bid=None):
        #print(self.num_records)
        #self.lineage += 1
        insert_index = self.num_records
        self.num_records += 1
        l = insert_index * COL_SIZE
        r = l + COL_SIZE
        self.data[l:r] = value.to_bytes(8, 'big')
        
        if not bid is None:
            self.bids[l:r] = bid.to_bytes(8, 'big')
        return insert_index

    """
    TODO: Do we need to check if the offset valid?
    """
    def read(self, offset, corr_bid=False):
        l = offset * COL_SIZE
        r = l + COL_SIZE
        value = int.from_bytes(self.data[l:r], 'big')
        if corr_bid is False:
            return value
        else:
            return value, int.from_bytes(self.bids[l:r],'big')

    def inplace_update(self, offset, val):
        l = offset * COL_SIZE
        r = l + COL_SIZE
        self.data[l:r] = val.to_bytes(8, 'big')

    
