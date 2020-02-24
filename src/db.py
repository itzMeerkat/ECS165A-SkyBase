from .table import Table
from .bufferpool import *
import os

class Database():

    def __init__(self):
        self.tables = []
        self.next_rid = 0 #???
        self.file_handler = []
        pass

    def open(self, db_name):
        file_path = db_name.replace("~/", "")
        bp_meta = file_path + "_bp_meta"
        tp_meta = file_path + "_tp_meta"
        bp_file = file_path + "_bp"
        tp_file = file_path + "_tp"

        try:
            bp_meta_handler = open(bp_meta, 'r+')
        except IOError:
        # If not exists, create the file
            bp_meta_handler = open(bp_meta, 'w+')

        try:
            tp_meta_handler = open(tp_meta, 'r+')
        except IOError:
        # If not exists, create the file
            tp_meta_handler = open(tp_meta, 'w+')

        try:
            bp_handler = open(bp_file, 'r+')
        except IOError:
        # If not exists, create the file
            bp_handler = open(bp_file, 'w+')

        try:
            tp_handler = open(tp_file, 'r+')
        except IOError:
        # If not exists, create the file
            tp_handler = open(tp_file, 'w+')

        self.file_handler = [bp_meta_handler, tp_meta_handler, bp_handler, tp_handler]

    def close(self):
        for handler in self.file_handler:
            handler.close()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key, self.file_handler)
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

    