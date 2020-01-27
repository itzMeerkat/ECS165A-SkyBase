from .config import *

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)
        self.free_index = [i for i in range(511, 0, -1)]
        self.MAX_RECORDS = PAGE_SIZE / COL_SIZE

    def has_capacity(self):
        if self.MAX_RECORDS > self.num_records:
            return True
        return False

    '''
    Each write will write a 64-bit long data into page
    TODO: benchmark of byte conversion
    '''
    def write(self, value):
        insert_index = self.free_index.pop()
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


    def remove(self, index):
        self.free_index.append(index)
        self.num_records -= 1
        return