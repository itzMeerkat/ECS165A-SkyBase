from .config import *

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)
        self.bids = bytearray(PAGE_SIZE)
        self.lineage = 0 # remeber to put this on the first spot in the page when flushing

    def has_capacity(self):
        if MAX_RECORDS > self.num_records:
            return True
        return False

    '''
    Each write will write a 64-bit long data into page
    TODO: benchmark of byte conversion
    '''
    def write(self, value, bid):
        #print(self.num_records)
        #self.lineage += 1
        insert_index = self.num_records
        self.num_records += 1
        l = insert_index * COL_SIZE
        r = l + COL_SIZE
        self.data[l:r] = value.to_bytes(8, 'big')

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

    def to_disk(self):
        self.data[:COL_SIZE] = self.lineage.to_bytes(8, 'big')
        return self.data, self.bids

    def from_disk(self, d, b):
        self.data = d
        self.bids = b

        for i in range(1, MAX_RECORDS+1):
            _t = int.from_bytes(b[i*COL_SIZE:(i+1)*COL_SIZE])
            if _t > 0:
                self.num_records += 1


    
