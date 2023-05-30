import time
import utils
from query_storage import query_storage
from chars_config import chars_config

def nyse_size_cutoffs(conn,cursor,data,out):
    # initiate utilities
    queries = query_storage.query_bank["nyse_size_cutoffs"]
    util_funcs = utils.utils(conn, cursor)
    executor=utils.executor(conn,cursor)
    char_collections=chars_config()
    print("Starting process nyse_size_cutoffs at time " + time.asctime())

    #query1: create table nyse_stocks
    #        select from {data}
    executor.execute_and_commit(queries["query1"])

    #query2: create table {out}
    #        summary statistics from nyse_stocks
    executor.execute_and_commit(queries["query2"])
    util_funcs.delete_table(["nyse_stocks"])