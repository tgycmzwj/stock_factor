import time
import utils
from query_storage import query_storage

def compustat_fx(conn,cursor):
    queries=query_storage.query_bank["compustat_fx"]
    util_funcs = utils.utils(conn, cursor)
    executor=utils.executor(conn,cursor)
    print("Starting processing compustat_fx at time " + time.asctime())
    #query1: create table __fx1
    #        join table comp.exrt_dly with self to compute exchange rate against dollar
    executor.execute_and_commit(queries["query1"])
    #query2: insert into table __fx1
    #        exchange rate for dollar itself
    executor.execute_and_commit(queries["query2"])
    #query3: create table __fx2
    #        sort table __fx1
    executor.execute_and_commit(queries["query3"])
    #query4: create table __fx3
    #        add variable following (the next date in data)
    executor.execute_and_commit(queries["query4"])
    #query5: update table __fx3
    #        set following to be date+1 if missing
    executor.execute_and_commit(queries["query5"])
    #query6: update table __fx3
    #        set variable n=following-date
    executor.execute_and_commit(queries["query6"])
    #query7: update table __fx3------this is wrong!!!
    #        set variable date to be
    util_funcs.duplicate_records(table_in="__fx3",table_out="__fx4",num="n")
    executor.execute_and_commit(queries["query7"])
    #query9: clean up
    util_funcs.delete_column([["__fx4","datadate"],["__fx4","following"],["__fx4","n"],["__fx4","value"]])
    util_funcs.sort_and_remove_duplicates(table_in="__fx4",table_out="fx",sortvar="curcdd,date",idvar="curcdd,date")
    util_funcs.delete_table(["__fx1","__fx2","__fx3","__fx4"])
