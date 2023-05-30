import time
import utils
from query_storage import query_storage
from comp_hgics import comp_hgics

def hgics_join(conn,cursor,out):
    queries=query_storage.query_bank["comp_hgics"]
    util_funcs = utils.utils(conn, cursor)
    executor=utils.executor(conn,cursor)
    print("Starting processing compustat_fx at time " + time.asctime())

    comp_hgics(conn,cursor,lib="co_hgic",out="na_hgics")
    comp_hgics(conn,cursor,lib="g_co_hgic",out="g_hgics")

    # query1: create table gjoin1
    #        join na_hgics and g_hgics
    executor.execute_and_commit(queries["query1"].format(lib))
    # query2: create table gjoin2
    #        select from gjoin2
    executor.execute_and_commit(queries["query2"].format(lib))
    #clean up
    util_funcs.delete_column([["gjoin2","na_gvkey"],["gjoin2","na_date"],
                              ["gjoin2","na_gics"],["gjoin2","g_gvkey"],
                              ["gjoin2","g_date"],["gjoin2","g_gics"]])
    util_funcs.sort_and_remove_duplicates(table_in="gjoin2",table_out=out,sortvar="gvkey,date",idvar="gvkey,date")
    util_funcs.delete_table(["na_hgics","g_hgics","gjoin1","gjoin2"])


