import os
import random
from datetime import datetime

class Logger():
    #format of query string
    # [delimiter, transaction_id, [query_statement], (old_value, new_value), timestamp]
    def __init__(self):
        file_name = "log_file"
        if os.path.exists(file_name):
            self.log_fh = open(file_name, 'r+')
        else:
            self.log_fh = open(file_name, 'w+')
        self.query_actions = ["delete", "insert", "select", "update", "sum", "increment"]
        self.delimiter = "!00! "

    #query[0] will have the transaction id
    #query[1] will be the query_id
    #check if the query list sent in is valid and if it is will return a string
    def check_build_string(self, query):
        if(query[1][0] not in self.query_actions):
            return None
        query_str = ""
        for i in range(len(query)):
            if i == len(query)-1:
                query_str += str(query[i])
            else:
                new_i = str(query[i]) + " "
                query_str += new_i
        return query_str

    #log is at the level of a query, so the query passed in is the rest of the columns
    #we assume the first entry of query is 
    def first_add(self, query):
        query_str = self.check_build_string(query)
        if query_str == "":
            print("ERROR SOMETHING WENT HORRIBLY WRONG")
            return False
        self.log_fh.seek(0, 2)
        self.log_fh.write(self.delimiter + query_str + " " + str(datetime.now()) + "\n")
        return True

    #looking through 
    #possible method: instead of overwriting can we just delete the line if the transaction is finished???/
    #duplicate queries?
    def finished_add(self, query):
        query_str = self.check_build_string(query)
        query_str = self.delimiter + query_str
        if query_str == "":
            print("ERROR SOMETHING WENT HORRIBLY WRONG")
            return False
        self.log_fh.seek(0)
        line = self.log_fh.readline()
        while line:
            pos = self.log_fh.tell()
            if query_str in line:
                line = line.strip()
                new_line = line.replace("!00!", "!11!")
                new_line += "\n"
                self.log_fh.seek(pos-len(line) -1)
                self.log_fh.write(new_line)
                return True
            line = self.log_fh.readline()
        return False

    def find_aborted(self, query):
        aborted = []
        transaction_id = query[0]
        self.log_fh.seek(0)
        if transaction_id is None:
            for line in self.log_fh:
                if self.delimiter in line:
                    line = line.strip()
                    aborted.append(line[5:])
        else:
            for line in self.log_fh:
                if self.delimiter in line and str(transaction_id) in line:
                    line = line.strip()
                    aborted.append(line[5:])
        return aborted

transaction_id = 1445551
query_id = 874917017097107
send_query = [transaction_id, ["insert"], (123, None, 123)]
logger = Logger()
logger.first_add(send_query)
#logger.finished_add(send_query)
#res = logger.find_aborted(send_query)
#for i in res:
#    print(i)