import time
import utils


def prepare_comp_sf(conn,cursor,queries,freq="m"):
    util_funcs = utils.utils(conn,cursor)
    print("Starting processing freq={freq} at time ".format(freq=freq) + time.asctime())


    # #query1:
    # cursor.execute(queries["query1"].format(freq=freq))
    # cursor.fetchall()
    # print("finished query 1 at time "+time.asctime())
    #
    # #query2:
    # util_funcs.sort_and_remove_duplicates(table_in="__firm_shares1",table_out="__temp",sortvar="gvkey,datadate DESC",idvar="gvkey,datadate")
    #
    # #query3
    # cursor.execute(queries["query3_1"])
    # cursor.fetchall()
    # print("finished query 3_1 at time "+time.asctime())
    #
    # cursor.execute(queries["query3_2"])
    # cursor.fetchall()
    # print("finished query 3_2 at time "+time.asctime())
    #
    # cursor.execute(queries["query3_3"])
    # cursor.fetchall()
    # print("finished query 3_3 at time "+time.asctime())
    #
    # util_funcs.sort_and_remove_duplicates(table_in="__temp3",table_out="__firm_shares2",sortvar="gvkey,datadate,ddate",idvar="gvkey,datadate,ddate")

    #query4

    util_funcs.delete_table(["__temp","__temp1","__temp2","__temp3"])
    util_funcs.delete_column([["__firm_shares2","following"],["__firm_shares2","forward_max"],
                              ["__firm_shares2","n"],["__firm_shares2","row_number"]])
    util_funcs.rename_column([["__firm_shares2","following_new","following"]])

    #query5
    cursor.execute(queries["query5"])
    cursor.fetchall()
    print("finished query 5 at time " + time.asctime())

    #query6
    cursor.execute(queries["query6"])
    cursor.fetchall()
    print("finished query 6 at time " + time.asctime())

    #query7
    cursor.execute(queries["query7"])
    cursor.fetchall()
    print("finished query 7 at time " + time.asctime())

    #query8
    cursor.execute(queries["query8"])
    cursor.fetchall()
    print("finished query 8 at time " + time.asctime())


    #query9
    cursor.execute(queries["query9"])
    cursor.fetchall()
    print("finished query 9 at time " + time.asctime())

    #query10
    cursor.execute(queries["query10"])
    cursor.fetchall()
    print("finished query 10 at time " + time.asctime())

    #query11
    util_funcs.delete_column([["__comp_dsf3","div"],["__comp_dsf3","divd"],
                              ["__comp_dsf3","divsp"],["__comp_dsf3","fx_div"],
                              ["__comp_dsf3","curcddv"],["__comp_dsf3","prc_high_lcl"],
                              ["__comp_dsf3","prc_low_lcl"]])




