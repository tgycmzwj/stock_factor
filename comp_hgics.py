import time
import utils
from query_storage import query_storage

def comp_hgics(conn,cursor,lib,out):
    queries=query_storage.query_bank["comp_hgics"]
    util_funcs = utils.utils(conn, cursor)
    executor=utils.executor(conn,cursor)
    print("Starting processing compustat_fx at time " + time.asctime())

    #query1: create table gic1
    #        select from comp_{lib}
    executor.execute_and_commit(queries["query1"].format(lib))
    util_funcs.sort_table(table="gic1",sortvar="gvkey,indfrom")

    #query2: create table gic2
    #        set missing gics, row_number
    executor.execute_and_commit(queries["query2"])

    #query3: create table gic3
    #        set indthru
    executor.execute_and_commit(queries["query3"])

    #query4: create table gic4
    #        set gic_diff
    executor.execute_and_commit(queries["query4"])

    util_funcs.duplicate_records(table_in="gic4",table_out="gic5",num="gic_diff")
    util_funcs.rename_table([["gic5","indfrom","date"]])
    util_funcs.delete_column([["gic5","indfrom"],["gic5","indthru"],["gic5","gic_diff"],["gic5","n"]])
    util_funcs.sort_and_remove_duplicates(table_in="gic5",table_out=out,sortvar="gvkey,date",idvar="gvkey,date")
    util_funcs.delete_table(["gic1","gic2","gic3","gic4","gic5","gic6"])