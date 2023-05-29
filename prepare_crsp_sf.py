import time
import utils
from query_storage import query_storage

def prepare_crsp_sf(conn,cursor,freq="m"):
    util_funcs=utils.utils(conn,cursor)
    queries = query_storage.query_bank["prepare_comp_sf"]
    executor=utils.executor(conn,cursor)
    print("Starting processing freq={freq} at time ".format(freq=freq) + time.asctime())

    #query1: create table __crsp_sf1
    executor.execute_and_commit(queries["query1"].format(freq=freq))
    #query2: adjust volatility [update]
    executor.execute_and_commit(queries["query2"])
    #query3: sort __crsp_sf1 by permno,date
    util_funcs.sort_table(table="__crsp_sf1",sortvar=["permno","date"])
    #query4: create table __crsp_sf2
    executor.execute_and_commit(queries["query4"])
    #query5: create table __crsp_sf3
    if freq=="m":
        executor.execute_and_commit(queries["query5_m"].format(freq=freq))
    else:
        executor.execute_and_commit(queries["query5_d"].format(freq=freq))
    #query6: create table __crsp_sf4_temp
    executor.execute_and_commit(queries["query6"])
    util_funcs.delete_column([["__crsp_sf4","ret"],["__crsp_sf4","dlret"],["__crsp_sf4","dlstcd"]])
    util_funcs.rename_table([["__crsp_sf4","dlret_new","dlret"],["__crsp_sf4","ret_new","ret"]])
    #query7: create table __crsp_sf5
    if freq=="m":
        scale="21"
    else:
        scale="1"
    executor.execute_and_commit(queries["query7"].format(scale=scale))
    #query8: create table __crsp_sf6
    executor.execute_and_commit(queries["query8"])
    #query9: update table __crsp_sf6
    if freq=="m":
        executor.execute_and_commit(queries["query9"])
    #query10: create table __crsp_m/dsf
    cursor.execute(queries["query10"].format(freq=freq))
    util_funcs.delete_table(["__crsp_sf1","__crsp_sf1_sorted","__crsp_sf2","__crsp_sf3",
                             "__crsp_sf4","__crsp_sf4_temp","__crsp_sf5","__crsp_sf6"])
    print("finished")
