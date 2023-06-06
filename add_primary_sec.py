import time
import utils
from query_storage import query_storage
from chars_config import chars_config


def add_primary_sec(conn,cursor,data,out,date_var):
    # initiate utilities
    queries = query_storage.query_bank["add_primary_sec"]
    util_funcs = utils.utils(conn, cursor)
    executor=utils.executor(conn,cursor)
    char_collections=chars_config()
    print("Starting process add_primary_sec at time " + time.asctime())

    # #query1: create table __prihistrow
    # #        select from comp_g_sec_history
    # executor.execute_and_commit(queries["query1"])
    #
    # #query2: create table __prihistrow
    # #        select from comp_sec_history
    # executor.execute_and_commit(queries["query2"])
    #
    # #query3: create table __prihistcan
    # #        select from comp_sec_history
    # executor.execute_and_commit(queries["query3"])

    #query4: create table __header
    #        union comp_company and comp_g_company
    executor.execute_and_commit(queries["query4"])
    util_funcs.sort_and_remove_duplicates(table_in="__header",table_out="__header",sortvar="gvkey",idvar="gvkey")

    #query5: create table __data1
    #        join {data}, __prihistrow, __prihistusa, __prihistcan, __header
    executor.execute_and_commit(queries["query5"].format(data=data,date_var=date_var))

    #query6: create table __data2
    #        select from __data1
    executor.execute_and_commit(queries["query6"])
    #clean up
    util_funcs.delete_column([["__data2","prihistrow"],["__data2","prihistusa"],
                              ["__data2","prihistcan"]])
    util_funcs.rename_table([["__data2",out]])
    util_funcs.delete_table(["__prihistrow","__prihistusa","__prihistcan",
                             "__header","__data1"])



