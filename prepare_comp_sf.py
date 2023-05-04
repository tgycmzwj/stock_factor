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
    # util_funcs.sort_and_remove_duplicates(table_in="__firm_shares1",table_out="__temp",sortvar="gvkey,datadate DESC",idvar="gvkey,datadate")
    #
    #query2
    # cursor.execute(queries["query2"])
    # cursor.fetchall()
    # print("finished query 2 at time "+time.asctime())

    # cursor.execute(queries["query3"])
    # cursor.fetchall()
    # print("finished query 3 at time "+time.asctime())

    cursor.execute(queries["query4"])
    cursor.fetchall()
    print("finished query 4 at time "+time.asctime())

    util_funcs.sort_and_remove_duplicates(table_in="__temp3",table_out="__firm_shares2",sortvar="gvkey,datadate,ddate",idvar="gvkey,datadate,ddate")
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

    #query11
    cursor.execute(queries["query11"])
    cursor.fetchall()
    print("finished query 11 at time " + time.asctime())

    #query12
    cursor.execute(queries["query12"])
    cursor.fetchall()
    print("finished query 12 at time " + time.asctime())

    #query13
    util_funcs.delete_column([["__comp_msf2","cshtrd"],["__comp_msf2","div_tot"],
                              ["__comp_msf2","div_cash"],["__comp_msf2","div_spc"],
                              ["__comp_msf2","dolvol"],["__comp_msf2","prc_high"],
                              ["__comp_msf2","prc_low"]])
    util_funcs.rename_column([["__comp_msf2","div_totm","div_tot"],
                              ["__comp_msf2","div_cashm","div_cash"],
                              ["__comp_msf2","div_spcm","div_spc"],
                              ["__comp_msf2","dolvolm","dolvol"],
                              ["__comp_msf2","prc_highm","prc_high"],
                              ["__comp_msf2","prc_lowm","prc_low"]])

    #query14
    cursor.execute(queries["query14"])
    cursor.fetchall()
    print("finished query 14 at time " + time.asctime())

    #query15
    cursor.execute(queries["query15"])
    cursor.fetchall()
    print("finished query 15 at time " + time.asctime())

    #query16
    cursor.execute(queries["query16"])
    cursor.fetchall()
    print("finished query 16 at time " + time.asctime())

    #query17
    cursor.execute(queries["query17"])
    conn.commit()
    cursor.fetchall()
    print("finished query 17 at time " + time.asctime())

    #query18
    cursor.execute(queries["query18"])
    conn.commit()
    cursor.fetchall()
    print("finished query 18 at time " + time.asctime())

    #query19
    cursor.execute(queries["query19"])
    cursor.fetchall()
    print("finished query 19 at time " + time.asctime())

    #query20
    util_funcs.delete_column([["__comp_secm2","dvpsxm"],["__comp_secm2","fx_div"],
                              ["__comp_secm2","curcddvm"],["__comp_secm2","prc_high"],
                              ["__comp_secm2","prc_low"],["__comp_secm2","div_cash"],
                              ["__comp_secm2","div_spc"]])
    util_funcs.rename_column([["__comp_secm2","prc_high_new","prc_high"],
                              ["__comp_secm2","prc_low_new","prc_low"],
                              ["__comp_secm2","div_cash_new","div_cash"],
                              ["__comp_secm2","div_spc_new","div_spc"]])

    #query21
    cursor.execute(queries["query21"])
    cursor.fetchall()
    print("finished query 21 at time " + time.asctime())

    #query22
    cursor.execute(queries["query22"])
    cursor.fetchall()
    print("finished query 22 at time " + time.asctime())

    #query23
    cursor.execute(queries["query23"])
    cursor.fetchall()
    print("finished query 23 at time " + time.asctime())

    #query24
    cursor.execute(queries["query24"])
    cursor.fetchall()
    print("finished query 24 at time " + time.asctime())

    #query25
    cursor.execute(queries["query25"])
    cursor.fetchall()
    print("finished query 25 at time " + time.asctime())

    #query26
    cursor.execute(queries["query26"])
    cursor.fetchall()
    print("finished query 26 at time " + time.asctime())

    #query27
    cursor.execute(queries["query27"])
    conn.commit()
    cursor.fetchall()
    print("finished query 27 at time " + time.asctime())

    #query28
    cursor.execute(queries["query28"])
    conn.commit()
    cursor.fetchall()
    print("finished query 28 at time " + time.asctime())

    #query29
    cursor.execute(queries["query29"])
    cursor.fetchall()
    print("finished query 29 at time " + time.asctime())

    #query30
    cursor.execute(queries["query30"])
    cursor.fetchall()
    print("finished query 30 at time " + time.asctime())

    #query31
    cursor.execute(queries["query31"])
    cursor.fetchall()
    print("finished query 31 at time " + time.asctime())

    #query70
    cursor.execute(queries["query70"])
    cursor.fetchall()
    print("finished query 70 at time " + time.asctime())

    #query75
    cursor.execute(queries["query75"])
    cursor.fetchall()
    print("finished query 75 at time " + time.asctime())

    #query80
    cursor.execute(queries["query80"])
    cursor.fetchall()
    print("finished query 80 at time " + time.asctime())

    #query85
    cursor.execute(queries["query85"])
    cursor.fetchall()
    print("finished query 85 at time " + time.asctime())

    #query86
    cursor.execute(queries["query86"])
    conn.commit()
    cursor.fetchall()
    print("finished query 86 at time " + time.asctime())

    #query87
    cursor.execute(queries["query87"])
    conn.commit()
    cursor.fetchall()
    print("finished query 87 at time " + time.asctime())

    #query90
    cursor.execute(queries["query90"])
    cursor.fetchall()
    print("finished query 90 at time " + time.asctime())

    #query100
    cursor.execute(queries["query100"])
    cursor.fetchall()
    print("finished query 100 at time " + time.asctime())

    #query101
    util_funcs.delete_table(["__firm_shares1","__firm_shares2","fx",
                             "__comp_dsf_na","__comp_dsf_global","__comp_dsf1",
                             "__comp_dsf2","__comp_dsf3","__returns",
                             "__sec_info","__delist1","__delist2",
                             "__delist3","__comp_sf1","__comp_sf2",
                             "__comp_sf3","__comp_sf4","__comp_sf5",
                             "__comp_sf6","__exchanges"])


