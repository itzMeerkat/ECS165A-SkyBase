from .table import Table
from .bufferpool import *
import os

class Database():

    def __init__(self):
        self.tables = []
        self.next_rid = 0 #???
        self.bp_handler = None
        self.tp_handler = None
        pass

    def open(self, db_name):
        file_path = db_name.replace("~/", "")
        base_page_file = file_path + "_base_page_file"
        tail_page_file = file_path + "_tail_page_file"
        try:
            bp_handler = open(self.base_page_file, 'a+')
        except IOError:
        # If not exists, create the file
            bp_handler = open(self.base_page_file, 'w+')

        try:
            tp_handler = open(self.tail_page_file, 'a+')
        except IOError:
        # If not exists, create the file
            tp_handler = open(self.tail_page_file, 'w+')

        self.bp_handler = bp_handler
        self.tp_handler = tp_handler

    def close(self):
        self.bp_handler.close()
        self.tp_handler.close()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key, self.bp_handler, self.tp_handler)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass

    #???
    def _get_rid(self):
        self.next_rid += 1
        return self.next_rid

    