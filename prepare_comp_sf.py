import time
import utils
from compustat_fx import compustat_fx
from query_storage import query_storage


def prepare_comp_sf(conn,cursor,freq="m"):
    queries=query_storage.query_bank["prepare_comp_sf"]
    freq="month" if freq=="m" else "day"
    util_funcs=utils.utils(conn,cursor)
    executor=utils.executor(conn,cursor)
    print("Starting processing freq={freq} at time ".format(freq=freq) + time.asctime())

    # #query1: create table __firm_shares1
    # cursor.execute(queries["query1"].format(freq=freq))
    # cursor.fetchall()
    # print("finished query 1 at time "+time.asctime())
    # util_funcs.sort_and_remove_duplicates(table_in="__firm_shares1",table_out="__temp",sortvar="gvkey,datadate DESC",idvar="gvkey,datadate")
    #
    # #query2: create table __temp1
    # cursor.execute(queries["query2"])
    # cursor.fetchall()
    # print("finished query 2 at time "+time.asctime())
    #
    # #query3: create table __temp2
    # cursor.execute(queries["query3"].format(freq=freq))
    # cursor.fetchall()
    # print("finished query 3 at time "+time.asctime())
    # util_funcs.delete_column([["__temp2","row_number"]])
    # util_funcs.duplicate_records(table_in="__temp2",table_out="__temp3",num="n")
    #
    # #query4: crate table __temp4
    # cursor.execute(queries["query4"].format(freq=freq))
    # cursor.fetchall()
    # conn.commit()
    # print("finished query 4 at time "+time.asctime())
    # util_funcs.sort_and_remove_duplicates(table_in="__temp4",table_out="__firm_shares2",sortvar="gvkey,datadate,ddate",idvar="gvkey,datadate,ddate")
    # util_funcs.delete_table(["__temp","__temp1","__temp2","__temp3","__temp4"])
    # util_funcs.delete_column([["__firm_shares2","following"],["__firm_shares2","forward_max"],
    #                           ["__firm_shares2","n"],["__firm_shares2","value"],["__firm_shares2","following_new"]])
    #
    # #query5: create table __comp_dsf_na
    # cursor.execute(queries["query5"])
    # cursor.fetchall()
    # print("finished query 5 at time " + time.asctime())
    #
    # #query6: update table __comp_dsf_na
    # cursor.execute(queries["query6"])
    # cursor.fetchall()
    # conn.commit()
    # print("finished query 6 at time " + time.asctime())
    #
    # #query7: create table __comp_dsf_global
    # cursor.execute(queries["query7"])
    # cursor.fetchall()
    # print("finished query 7 at time " + time.asctime())

    # #query8: create table __comp_dsf1
    # cursor.execute(queries["query8"])
    # cursor.fetchall()
    # print("finished query 8 at time " + time.asctime())
    #
    # #helper: prepare exchange rate table
    # compustat_fx(conn,cursor)
    #
    # #query9: create table __comp_dsf2
    # cursor.execute(queries["query9"])
    # cursor.fetchall()
    # print("finished query 9 at time " + time.asctime())
    #
    # #query10: create table __comp_dsf3
    # cursor.execute(queries["query10"])
    # cursor.fetchall()
    # print("finished query 10 at time " + time.asctime())
    #
    # #query11
    # util_funcs.delete_column([["__comp_dsf3","div"],["__comp_dsf3","divd"],
    #                           ["__comp_dsf3","divsp"],["__comp_dsf3","fx_div"],
    #                           ["__comp_dsf3","curcddv"],["__comp_dsf3","prc_high_lcl"],
    #                           ["__comp_dsf3","prc_low_lcl"]])
    #


    if freq=="m" or freq=="d":
        iter_max=1
    else:
        iter_max=2
    for iter in range(iter_max):
        if freq=="m" or freq=="d":
            freq_use=freq
        elif iter==1:
            freq_use="d"
        else:
            freq_use="m"

        if freq_use=="m":
            # #query11: create table __comp_msf1_temp
            # cursor.execute(queries["query11"])
            # cursor.fetchall()
            # print("finished query 11 at time " + time.asctime())

            # # query12: create table __comp_msf1
            # cursor.execute(queries["query12"])
            # cursor.fetchall()
            # print("finished query 12 at time " + time.asctime())
            #
            # # query13: create table __comp_msf2
            # cursor.execute(queries["query13"])
            # cursor.fetchall()
            # print("finished query13 at time "+time.asctime())
            #
            # util_funcs.delete_column([["__comp_msf2", "cshtrd"], ["__comp_msf2", "div_tot"],
            #                           ["__comp_msf2", "div_cash"], ["__comp_msf2", "div_spc"],
            #                           ["__comp_msf2", "dolvol"], ["__comp_msf2", "prc_high"],
            #                           ["__comp_msf2", "prc_low"]])
            # util_funcs.rename_column([["__comp_msf2", "div_totm", "div_tot"],
            #                           ["__comp_msf2", "div_cashm", "div_cash"],
            #                           ["__comp_msf2", "div_spcm", "div_spc"],
            #                           ["__comp_msf2", "dolvolm", "dolvol"],
            #                           ["__comp_msf2", "prc_highm", "prc_high"],
            #                           ["__comp_msf2", "prc_lowm", "prc_low"]])
            #
            # #query14: create table __comp_msf3
            # cursor.execute(queries["query14"])
            # cursor.fetchall()
            # print("finished query 14 at time " + time.asctime())
            #
            # #query15: create table __comp_secm1
            # cursor.execute(queries["query15"])
            # cursor.fetchall()
            # print("finished query 15 at time " + time.asctime())
            #
            # #query16: update table __comp_secm1
            # executor.execute_and_commit(queries["query16"])
            #
            # #query17
            # executor.execute_and_commit(queries["query17"])
            #
            # #query18
            # executor.execute_and_commit(queries["query18"])
            # conn.commit()
            # cursor.fetchall()
            # print("finished query 18 at time " + time.asctime())
            #
            # #query19
            executor.execute_and_commit(queries["query19"])
            # cursor.execute(queries["query19"])
            # cursor.fetchall()
            # print("finished query 19 at time " + time.asctime())
            #
            # #query20
            # util_funcs.delete_column([["__comp_secm2","dvpsxm"],["__comp_secm2","fx_div"],
            #                           ["__comp_secm2","curcddvm"],["__comp_secm2","prc_high"],
            #                           ["__comp_secm2","prc_low"],["__comp_secm2","div_cash"],
            #                           ["__comp_secm2","div_spc"]])
            # util_funcs.rename_column([["__comp_secm2","prc_high_new","prc_high"],
            #                           ["__comp_secm2","prc_low_new","prc_low"],
            #                           ["__comp_secm2","div_cash_new","div_cash"],
            #                           ["__comp_secm2","div_spc_new","div_spc"]])
            #
    # #query21
    # cursor.execute(queries["query21"])
    # cursor.fetchall()
    # print("finished query 21 at time " + time.asctime())
    #
    # #query22
    # cursor.execute(queries["query22"])
    # cursor.fetchall()
    # print("finished query 22 at time " + time.asctime())
    #
    # #query23
    # cursor.execute(queries["query23"])
    # cursor.fetchall()
    # print("finished query 23 at time " + time.asctime())
    #
    # #query24
    # cursor.execute(queries["query24"])
    # cursor.fetchall()
    # print("finished query 24 at time " + time.asctime())
    #
    # #query25
    # cursor.execute(queries["query25"])
    # cursor.fetchall()
    # print("finished query 25 at time " + time.asctime())
    #
    # #query26
    # cursor.execute(queries["query26"])
    # cursor.fetchall()
    # print("finished query 26 at time " + time.asctime())
    #
    # #query27
    # cursor.execute(queries["query27"])
    # conn.commit()
    # cursor.fetchall()
    # print("finished query 27 at time " + time.asctime())
    #
    # #query28
    # cursor.execute(queries["query28"])
    # conn.commit()
    # cursor.fetchall()
    # print("finished query 28 at time " + time.asctime())
    #
    # #query29
    # cursor.execute(queries["query29"])
    # cursor.fetchall()
    # print("finished query 29 at time " + time.asctime())
    #
    # #query30
    # cursor.execute(queries["query30"])
    # cursor.fetchall()
    # print("finished query 30 at time " + time.asctime())
    #
    # #query31
    # cursor.execute(queries["query31"])
    # cursor.fetchall()
    # print("finished query 31 at time " + time.asctime())
    #
    # #query70
    # cursor.execute(queries["query70"])
    # cursor.fetchall()
    # print("finished query 70 at time " + time.asctime())
    #
    # #query75
    # cursor.execute(queries["query75"])
    # cursor.fetchall()
    # print("finished query 75 at time " + time.asctime())
    #
    # #query80
    # cursor.execute(queries["query80"])
    # cursor.fetchall()
    # print("finished query 80 at time " + time.asctime())
    #
    # #query85
    # cursor.execute(queries["query85"])
    # cursor.fetchall()
    # print("finished query 85 at time " + time.asctime())
    #
    # #query86
    # cursor.execute(queries["query86"])
    # conn.commit()
    # cursor.fetchall()
    # print("finished query 86 at time " + time.asctime())
    #
    # #query87
    # cursor.execute(queries["query87"])
    # conn.commit()
    # cursor.fetchall()
    # print("finished query 87 at time " + time.asctime())
    #
    # #query90
    # cursor.execute(queries["query90"])
    # cursor.fetchall()
    # print("finished query 90 at time " + time.asctime())
    #
    # #query100
    # cursor.execute(queries["query100"])
    # cursor.fetchall()
    # print("finished query 100 at time " + time.asctime())
    #
    # #query101
    # util_funcs.delete_table(["__firm_shares1","__firm_shares2","fx",
    #                          "__comp_dsf_na","__comp_dsf_global","__comp_dsf1",
    #                          "__comp_dsf2","__comp_dsf3","__returns",
    #                          "__sec_info","__delist1","__delist2",
    #                          "__delist3","__comp_sf1","__comp_sf2",
    #                          "__comp_sf3","__comp_sf4","__comp_sf5",
    #                          "__comp_sf6","__exchanges"])

