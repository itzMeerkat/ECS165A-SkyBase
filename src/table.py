from src.page import *
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns


class Column:
    def __init__(self):
        self.base_pages = []
        self.tail_pages = []

        self.num_base_pages = -1
        self.num_tail_pages = -1

        # {rid: (tailPageId,offset)}
        self.latest_lookup = {}
        # {rid: (basePageId,offset)}
        self.base_lookup = {}

        self._append_base_page()
        self._append_tail_page()


    def _append_base_page(self):
        self.base_pages.append(Page())
        self.num_base_pages += 1

    def _append_tail_page(self):
        self.tail_pages.append(Page())
        self.num_tail_pages += 1

    def get_latest_tail(self):
        latest_tail = self.tail_pages[self.num_tail_pages]
        if not latest_tail.has_capacity():
            self._append_tail_page()
            latest_tail = self.tail_pages[self.num_tail_pages]
        return latest_tail

    def get_latest_base(self):
        latest_base = self.base_pages[self.num_base_pages]
        if not latest_base.has_capacity():
            self._append_base_page()
            latest_base = self.base_pages[self.num_base_pages]
        return latest_base

    def lookup_latest(self, rid):
        if rid in self.latest_lookup:
            return 

    def write(self, val):
        latest = self.get_latest_tail()
        offset = latest.write(val)
        return offset

    def read(self, rid):


class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        pass

    def __merge(self):
        pass
 
