from .table import Table
from .bufferpool import *
import os
import json

class Database():

    def __init__(self):
        self.tables = []
        self.file_handler = []
        self.page_directory = {}
        self.pg_file_path = ""
        pass

    def open(self, db_name):
        file_path = db_name.replace("~/", "")
        bp_meta = file_path + "_bp_meta"
        tp_meta = file_path + "_tp_meta"
        bp_file = file_path + "_bp"
        tp_file = file_path + "_tp"
        pd_file = file_path + "page_direction.json"
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

        self.file_handler = [bp_meta_handler, tp_meta_handler, bp_handler, tp_handler]
        #update the page directory 
        self.pg_file_path = pd_file
        self.init_page_dir()
    
    def init_page_dir(self):
        with open(self.pg_file_path, 'r') as outfile:
            self.page_directory = json.load(outfile)
        self.rid = len(self.page_directory)
        
    def write_back_page_dir(self):
        with open(self.pg_file_path, "w") as outfile:
                pd_json = json.dumps(self.page_directory)
                outfile.write(pd_json)

    def close(self):
        for table in self.tables:
            table.bufferpool.write_back_all_dirty_page()

        for handler in self.file_handler:
            handler.close()
        self.write_back_page_dir()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key, self.file_handler,self.page_directory)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass

    """
    def _get_rid(self):
        self.next_rid += 1
        return self.next_rid
    """
    