import time
import utils


def prepare_comp_sf(conn,cursor,queries,freq="m"):
    util_funcs = utils.utils(conn,cursor)
    print("Starting processing freq={freq} at time ".format(freq=freq) + time.asctime())

    #query1:
    cursor.execute(queries["query1"].format(freq=freq))
    cursor.fetchall()
    print("finished query 1 at time "+time.asctime())

    #query2:
    util_funcs.sort_and_remove_duplicates(table_in="__firm_shares1",table_out="__temp",sortvar="gvkey,datadate DESC",idvar="gvkey,datadate")
    # cursor.execute(queries["query2"])
    # cursor.fetchall()
    # print("finished query 2 at time "+time.asctime())
    # cursor.execute(queries["query2_1"])
    # cursor.fetchall()
    # print("finished query 2_1 at time "+time.asctime())

    #query3
    cursor.execute(queries["query3_1"])
    cursor.fetchall()
    print("finished query 3_1 at time "+time.asctime())

    cursor.execute(queries["query3_2"])
    cursor.fetchall()
    print("finished query 3_2 at time "+time.asctime())

    cursor.execute(queries["query3_3"])
    cursor.fetchall()
    print("finished query 3_3 at time "+time.asctime())

    util_funcs.sort_and_remove_duplicates(table_in="__temp3",table_out="__firm_shares2",sortvar="gvkey,datadate,ddate",idvar="gvkey,datadate,ddate")
    # cursor.execute(queries["query3_4"])
    # cursor.fetchall()
    # print("finished query 3_3 at time "+time.asctime())

    #query4
    for i in range(1,10):
        cursor.execute(queries["query4_{}".format(i)])
        cursor.fetchall()
        print("finished query 4_{} at time ".format(i)+time.asctime())



