import time
import utils
from query_storage import query_storage

def prepare_crsp_sf(conn,cursor,freq="m"):
    util_funcs=utils.utils(conn,cursor)
    queries = query_storage.query_bank["prepare_comp_sf"]
    print("Starting processing freq={freq} at time ".format(freq=freq) + time.asctime())

    #query1: create table __crsp_sf1
    cursor.execute(queries["query1"].format(freq=freq))
    cursor.fetchall()
    print("finished query 1 at time "+time.asctime())

    #query2: adjust volatility [update]
    cursor.execute(queries["query2"])
    cursor.fetchall()
    conn.commit()
    print("finished query 2 at time "+time.asctime())

    #query3: sort __crsp_sf1 by permno,date
    util_funcs.sort_table(table="__crsp_sf1",sortvar=["permno","date"])
    print("finished query 3 at time "+time.asctime())

    #query4: create table __crsp_sf2
    cursor.execute(queries["query4"])
    cursor.fetchall()
    print("finished query 4 at time "+time.asctime())

    #query5: create table __crsp_sf3
    if freq=="m":
        cursor.execute(queries["query5_m"].format(freq=freq))
        cursor.fetchall()
    else:
        cursor.execute(queries["query5_d"].format(freq=freq))
        cursor.fetchall()
    print("finished query 5 at time "+time.asctime())

    #query6: create table __crsp_sf4_temp
    cursor.execute(queries["query6"])
    cursor.fetchall()
    print("finished query 6 at time "+time.asctime())
    util_funcs.delete_column([["__crsp_sf4","ret"],["__crsp_sf4","dlret"],["__crsp_sf4","dlstcd"]])
    util_funcs.rename_table([["__crsp_sf4","dlret_new","dlret"],["__crsp_sf4","ret_new","ret"]])

    #query7: create table __crsp_sf5
    if freq=="m":
        scale="21"
    else:
        scale="1"
    print("start")
    cursor.execute(queries["query7"].format(scale=scale))
    cursor.fetchall()
    print("finished query7 at time "+time.asctime())

    #query8: create table __crsp_sf6
    cursor.execute(queries["query8"])
    cursor.fetchall()
    print("finished query8 at time "+time.asctime())

    #query9: update table __crsp_sf6
    if freq=="m":
        cursor.execute(queries["query9"])
        cursor.fetchall()
        conn.commit()
        print("finished query9 at time "+time.asctime())

    #query10: create table __crsp_m/dsf
    cursor.execute(queries["query10"].format(freq=freq))
    cursor.fetchall()
    print("finished query10 at time "+time.asctime())
    util_funcs.delete_table(["__crsp_sf1","__crsp_sf1_sorted","__crsp_sf2","__crsp_sf3",
                             "__crsp_sf4","__crsp_sf4_temp","__crsp_sf5","__crsp_sf6"])
    print("finished")
