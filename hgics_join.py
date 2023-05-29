import time
import utils
from query_storage import query_storage

def hgics_join(conn,cursor,out):
    queries=query_storage.query_bank["comp_hgics"]
    util_funcs = utils.utils(conn, cursor)
    executor=utils.executor(conn,cursor)
    print("Starting processing compustat_fx at time " + time.asctime())