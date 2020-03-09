from .table import Table
from .column import Record
from .bufferpool import *

import os
import json

from threading import Lock
from .locks import RecordLocks

class RecordJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Record):
            return obj.toJSON()
        return json.JSONEncoder.default(self, obj)


class Database():
    def __init__(self):
        self.tables = {}
        self.file_handler = []
        self.page_directory = {}
        self.reverse_indirection = {}
        self.pg_file_path = ""
        self.rid = 1
        self.table_metas = {}

        self.RID_LOCK = Lock()

        self.rid_lock = RecordLocks()

    def get_next_rid(self):
        self.RID_LOCK.acquire()
        r = self.rid
        self.rid += 1
        self.RID_LOCK.release()
        return r

    def checkFileExist(self, *files):
        f = list(files)
        for i in f:
            if not os.path.exists(i):
                return False
        return True

    def open(self, db_name):
        file_path = db_name.replace("~/", "")
        bp_meta = file_path + "_bp_meta"
        tp_meta = file_path + "_tp_meta"
        bp_file = file_path + "_bp"
        tp_file = file_path + "_tp"
        pd_file = file_path + "page_directory.json"

        self.pd_file_path = pd_file
        self.init_page_dir(pd_file)

        bp_meta_handler = None
        tp_meta_handler = None
        bp_handler = None
        tp_handler = None

        if self.checkFileExist(bp_meta, tp_meta, bp_file, tp_file):
            bp_meta_handler = open(bp_meta, 'r+')
            tp_meta_handler = open(tp_meta, 'r+')
            bp_handler = open(bp_file, 'rb+')
            tp_handler = open(tp_file, 'rb+')

            self.file_handler = [bp_meta_handler, tp_meta_handler, bp_handler, tp_handler]
            for tn in self.table_metas:
                _meta = self.table_metas[tn]
                self.create_table(tn, _meta['num_columns'], _meta['key'], _meta['col_tail_lens'])
        else:
            bp_meta_handler = open(bp_meta, 'w+')
            tp_meta_handler = open(tp_meta, 'w+')
            bp_handler = open(bp_file, 'wb+')
            tp_handler = open(tp_file, 'wb+')
            self.file_handler = [bp_meta_handler, tp_meta_handler, bp_handler, tp_handler]
        
    
    def init_page_dir(self, path):
        if not os.path.exists(path):
            f = open(path, 'w+')
            f.close()
            self.page_directory = {}
            self.rid = 1
        else:
            with open(path, 'r+') as outfile:
                r_file = json.load(outfile)
                pd = r_file['page_directory']
                for k in pd:
                    _r = Record(None,None,None)
                    _r.fromJSON(pd[k])
                    self.page_directory[int(k)] = _r

                self.rid = len(self.page_directory) + 1

                self.table_metas = r_file['table_metas']
        
    def write_back_page_dir(self):
        with open(self.pd_file_path, "w") as outfile:
            table_meta = {}
            for k in self.tables:
                
                tl = {}
                for i, c in enumerate(self.tables[k].columns):
                    tl[i] = (c.len_tail,c.len_base)

                table_meta[k] = {'num_columns': self.tables[k].num_columns, 'key':self.tables[k].key, 'col_tail_lens':tl}
            big = {'table_metas': table_meta, 'page_directory': self.page_directory}
            big_json = json.dumps(big, cls=RecordJSONEncoder)
            outfile.write(big_json)

    def close(self):
        for table in self.tables:
            self.tables[table].bufferpool.write_back_all_dirty_page()

        for handler in self.file_handler:
            handler.close()
        self.write_back_page_dir()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key, _len=None):
        table = Table(name, num_columns, key, self.file_handler,
                      self.page_directory, self.reverse_indirection, self)
        if not _len is None:
            for i in _len:
                for j in _len[i][0]:
                    table.columns[int(i)].len_tail[int(j)] = _len[i][0][j]
                table.columns[int(i)].len_base = _len[i][1]
        self.tables[name] = table
        return table

    def get_table(self, name):
        if name in self.tables:
            return self.tables[name]
        return None

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass
