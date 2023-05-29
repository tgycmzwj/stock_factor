
import time
import utils
from query_storage import query_storage

def comp_exchanges(conn,cursor,out):
    # initiate utilities
    queries = query_storage.query_bank["comp_exchanges"]
    util_funcs = utils.utils(conn, cursor)
    executor=utils.executor(conn,cursor)
    print("Starting process comp_exchanges at time " + time.asctime())


