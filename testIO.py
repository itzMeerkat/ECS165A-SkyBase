import os
#testing opening closing files
file_path = 'test_file'
meta_path = 'meta_file'
try:
	meta_handler= open(meta_path, 'r+')
except IOError:
    # If not exists, create the file
    meta_handler = open(meta_path, 'w+')

try:
	file_handler= open(file_path, 'r+')
except IOError:
    # If not exists, create the file
    file_handler = open(file_path, 'w+')

pid = 3
pid = "(" + str(pid) + ","
data_array = [4 for i in range(4096)]
meta_handler.seek(0)
file_handler.seek(0)
begin = meta_handler.read().find(pid)

if begin == -1:
	#have to write to disk to file, and then to the meta data file
	#print("not found!")
	print("not found")
	file_handler.seek(0,2)
	meta_handler.write(pid + str(file_handler.tell()) + ")")
else:
	print("found")