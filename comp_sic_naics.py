import time
import utils
from query_storage import query_storage

def comp_sic_naics(conn,cursor,out,ff_num):
    queries=query_storage.query_bank["comp_hgics"]
    util_funcs = utils.utils(conn, cursor)
    executor=utils.executor(conn,cursor)
    print("Starting processing comp_sic_naics at time " + time.asctime())

    #query1: create table comp1
    #        select from comp_funda
    executor.execute_and_commit(queries["query1"])

    #query2: create table comp2
    #        select from comp1
    executor.execute_and_commit(queries["query2"])

    #query3: create table comp3
    #        select from comp_g_dunda
    executor.execute_and_commit(queries["query3"])

    #query4: create table comp4
    #        join comp2 and comp3
    executor.execute_and_commit(queries["query4"])

    #query5: create table comp5
    #        select from comp4
    executor.execute_and_commit(queries["query5"])
    util_funcs.delete_column([["comp5","gvkeya"],["comp5","gvkeyb"],
                              ["comp5","datea"],["comp5","dateb"],
                              ["comp5","sica"],["comp5","sicb"],
                              ["comp5","naicsa"],["comp5","naicsb"]])
    util_funcs.sort_table(table="comp5",sortvar="gvkey,date DESC")

    #query6: create table comp6
    #        select from comp5
    executor.execute_and_commit(queries["query6"])
    #query7: update table comp6
    #        set valid_to
    executor.execute_and_commit(queries["query7"])
    util_funcs.sort_table(table="comp6",sortvar="gvkey,date,valid_to")
    #query8: create table comp7
    #        select from comp6
    executor.execute_and_commit(queries["query8"])
    util_funcs.sort_table(table="comp7",sortvar="gvkey,date,valid_to")
    util_funcs.duplicate_records(table_in="comp7",table_out="comp8",num="comp_diff")
    util_funcs.sort_and_remove_duplicates(table_in="comp8",table_out=out,sortvar="gvkey,date",idvar="gvkey,date")
    util_funcs.delete_table(["comp1","comp2","comp3","comp4",
                             "comp5","comp6","comp7","comp8"])
