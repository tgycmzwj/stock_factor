import time
import utils
from query_storage import query_storage
from chars_config import chars_config


def add_primary_sec(conn,cursor,out):
    # initiate utilities
    queries = query_storage.query_bank["comp_exchanges"]
    util_funcs = utils.utils(conn, cursor)
    executor=utils.executor(conn,cursor)
    char_collections=chars_config()
    print("Starting process comp_exchanges at time " + time.asctime())

    #query1: create table permno0
    #        select from crsp_desnames
    executor.execute_and_commit(queries["query1"])
    #query2: update table permno0
    #        missing sic code
    executor.execute_and_commit(queries["query2"])
    #query3: create table permno0
    #        set column permno_diff
    executor.execute_and_commit(queries["query3"])
    util_funcs.sort_table(table="permno0",sortvar="permno,namedt,nameendt")
    util_funcs.duplicate_records(table_in="permno0",table_out="permno3",num="permno_diff")
    util_funcs.delete_column([["permno3","nameendt"],
                              ["permno3","permno_diff"],
                              ["permno3","n"]])

    #query4: create table permno4
    #        missing sic code
    executor.execute_and_commit(queries["query4"])
    util_funcs.sort_and_remove_duplicates(table_int="permno4",table_out=out,sortvar="permno,date",idvar="permno,date")
    util_funcs.delete_table(["permno0","permno3","permno4"])