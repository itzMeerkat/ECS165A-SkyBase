PAGE_SIZE = 4096
COL_SIZE = 8
META_COL_SIZE = 4

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

TO_BASE_PAGE = 0
TO_TAIL_PAGE = 1

BASE_PAGE=0
TAIL_PAGE=1

BUFFERPOOL_SIZE=1000

FAIL=-1
SUCCESS=0

WRITE = 1
READ = 0

PARTITION_SIZE = 16

MAX_RECORDS = PAGE_SIZE / COL_SIZE - 1