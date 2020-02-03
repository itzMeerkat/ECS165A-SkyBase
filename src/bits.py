from typing import List

class Bits:
    def __init__(self, bits_str: str):
        # bits here is in reverse order of bits_str
        self.bits = 0
        
        l = len(bits_str)
        self.size = l
        for i in range(l-1,-1,-1):
            if bits_str[i] == '1':
                self.bits |= 1
            self.bits <<= 1
        self.bits >>= 1

    def build_from_list(self, cols: List[int]):
        self.bits = 0

        l = len(cols)
        self.size = l
        for i in range(l-1, -1, -1):
            #print(i)
            if not cols[i] is None:
                self.bits |= 1
            self.bits <<= 1
        self.bits >>= 1
    
    def __iter__(self):
        self._bits = self.bits
        self._l = self.size
        return self

    def __next__(self):
        r = self._bits & 1
        self._bits = self._bits >> 1
        self._l -= 1
        if self._l >= 0:
            return r
        else:
            raise StopIteration
    

    """
    Merge will update current object
    """
    def merge(self, another):
        self.bits |= another.bits

    def set_meta(self, h):
        self.bits <<= 4
        self.bits |= 15
        self.size += 4

    def __getitem__(self, key):
        return self.bits & (1<<key)

    def to_bytes(self, size, order):
        return self.bits.to_bytes(size, order)
