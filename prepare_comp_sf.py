import time
import utils
from compustat_fx import compustat_fx
from query_storage import query_storage
from chars_config import chars_config

def prepare_comp_sf(conn,cursor,freq="m"):
    #initiate utilities
    queries=query_storage.query_bank["prepare_comp_sf"]
    freq="month" if freq=="m" else "day"
    util_funcs=utils.utils(conn,cursor)
    executor=utils.executor(conn,cursor)
    char_collections=chars_config()
    print("Starting processing freq={freq} at time ".format(freq=freq) + time.asctime())

    # #query1: create table __firm_shares1
    # #        select variables from comp.fundq union with funda
    # executor.execute_and_commit(queries["query1"].format(freq=freq))
    # #query2: drop duplicates for __firm_shares1, based on gvkey,datadate
    # util_funcs.sort_and_remove_duplicates(table_in="__firm_shares1",table_out="__temp",sortvar="gvkey,datadate DESC",idvar="gvkey,datadate")
    # #query2: create table __temp1
    # #        generate variable following, row_number, forward_max
    # executor.execute_and_commit(queries["query2"])
    # #query3: create table __temp2
    # #        create variable n, set first obs of following to NULL
    # executor.execute_and_commit(queries["query3"].format(freq=freq))
    # #query4: drop column row_number
    # util_funcs.delete_column([["__temp2","row_number"]])
    # #query5: duplicate the observation by N times (transform monthly to daily)
    # util_funcs.duplicate_records(table_in="__temp2",table_out="__temp3",num="n")
    # #query4: crate table __temp4
    # #        generate variable ddate
    # executor.execute_and_commit(queries["query4"].format(freq=freq))
    # #query5: remove duplicate for table __temp4
    # util_funcs.sort_and_remove_duplicates(table_in="__temp4",table_out="__firm_shares2",sortvar="gvkey,datadate,ddate",idvar="gvkey,datadate,ddate")
    # #query6: clean up: delete useless tables/columns
    # util_funcs.delete_table(["__temp","__temp1","__temp2","__temp3","__temp4"])
    # util_funcs.delete_column([["__firm_shares2","following"],["__firm_shares2","forward_max"],
    #                           ["__firm_shares2","n"],["__firm_shares2","value"],["__firm_shares2","following_new"]])
    # #query5: create table __comp_dsf_na
    # #        join comp_secd and __firm_shares2
    # executor.execute_and_commit(queries["query5"])
    # #query6: update table __comp_dsf_na
    # #        adjust column cshtrd
    # executor.execute_and_commit(queries["query6"])
    # #query7: create table __comp_dsf_global
    # #        select from comp_g_secd
    # executor.execute_and_commit(queries["query7"])
    # #query8: create table __comp_dsf1
    # #        union __comp_dsf_na and __comp_dsf_global
    # executor.execute_and_commit(queries["query8"])
    # #helper: prepare exchange rate table
    # compustat_fx(conn,cursor)
    # #query9: create table __comp_dsf2
    # #        join table __comp_dsf1 with fx
    # executor.execute_and_commit(queries["query9"])
    # #query10: create table __comp_dsf3
    # #         adjust price and dividend by fx
    # executor.execute_and_commit(queries["query10"])
    # #query11: clean up
    # util_funcs.delete_column([["__comp_dsf3","div"],["__comp_dsf3","divd"],
    #                           ["__comp_dsf3","divsp"],["__comp_dsf3","fx_div"],
    #                           ["__comp_dsf3","curcddv"],["__comp_dsf3","prc_high_lcl"],
    #                           ["__comp_dsf3","prc_low_lcl"]])
    #create daily, monthly or both datasets
    if freq=="month" or freq=="day":
        iter_max=1
    else:
        iter_max=2
    for iter in range(iter_max):
        if freq=="month" or freq=="day":
            freq_use=freq
        elif iter==1:
            freq_use="d"
        else:
            freq_use="m"
        if freq_use=="m":
            # #data in daily frequency, need to collapse to monthly first
            # #query11: create table __comp_msf1_temp
            # #         calculate variable eom
            # executor.execute_and_commit(queries["query11"])
            # #query12: create table __comp_msf1
            # #         collapse table to monthly
            # #         group table __comp_msf1_temp by gvkey,iid,eom
            # executor.execute_and_commit(queries["query12"])
            # # query13: create table __comp_msf2
            # #          select from __comp_msf1
            # executor.execute_and_commit(queries["query13"])
            # #clean up
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
            # #query14: create table __comp_msf3
            # #         select last observation in each month from __comp_msf2
            # executor.execute_and_commit(queries["query14"])
            # #query15: create table __comp_secm1
            # #         join table comp_secm, __firm_shares2, fx
            # executor.execute_and_commit(queries["query15"])
            # #query16: update table __comp_secm1
            # #         adjust cshtrm
            # executor.execute_and_commit(queries["query16"])
            # #query17: update table __comp_secm1
            # #         handle USD itself for curcdd
            # executor.execute_and_commit(queries["query17"])
            # #query18: update table __comp_secm1
            # #         handle USD itself for curcddvm
            # executor.execute_and_commit(queries["query18"])
            # #query19: create table __comp_secm2
            # #         adjust price/dividend with fx
            # executor.execute_and_commit(queries["query19"])
            # #query20: clean up
            # util_funcs.delete_column([["__comp_secm2","dvpsxm"],["__comp_secm2","fx_div"],
            #                           ["__comp_secm2","curcddvm"],["__comp_secm2","prc_high"],
            #                           ["__comp_secm2","prc_low"]])
            # util_funcs.rename_column([["__comp_secm2","prc_high_new","prc_high"],
            #                           ["__comp_secm2","prc_low_new","prc_low"],
            #                           ["__comp_secm2","div_cash_new","div_cash"],
            #                           ["__comp_secm2","div_spc_new","div_spc"]])
            # #query21: create table __comp_msf4
            # #         union __comp_msf3 and __comp_secm2
            # executor.execute_and_commit(queries["query21"].format(common_vars=",".join(char_collections.common_vars)))
            # #query22: create table __comp_msf5
            # #         group __comp_msf4 by gvkey, iid, eom
            # executor.execute_and_commit(queries["query22"])
            # #query23: create table __comp_msf6
            # #         sort and remove duplicates in __comp_msf5
            # util_funcs.sort_and_remove_duplicates(table_in="__comp_msf5",table_out="__comp_msf6",sortvar="gvkey,iid,eom",idvar="gvkey,iid,eom")
            # #clean up
            # util_funcs.delete_table(["__comp_msf1","__comp_msf2","__comp_msf3","__comp_msf4","__comp_msf5","__comp_secm1","__comp_secm2"])
            #config frequency
            period="month"
            base="__comp_msf6"
            out="comp_msf"
        elif freq_use=="d":
            #data already in daily frequency, no adjustment
            #config frequency
            period="day"
            base="__comp_dsf3"
            out="comp_dsf"
        # util_funcs.sort_and_remove_duplicates(table_in=base,table_out="__comp_sf1",sortvar="gvkey,iid,datadate",idvar="gvkey,iid,datadate")
        # # #query25: create table __returns_temp
        # #           filter __comp_sf1
        # executor.execute_and_commit(queries["query25"])
        # # #query26: create table __returns
        # #           select from __returns_temp
        # executor.execute_and_commit(queries["query26"])
        # # #query27: update table __returns
        # #           set the first observation in each group to be NULL
        # executor.execute_and_commit(queries["query27"])
        # # #query28: update table __returns
        # #           handle situations where currency code changes
        # executor.execute_and_commit(queries["query28"])
        # executor.execute_and_commit(queries["query28_1"])
        # # #clean up
        # util_funcs.keep_column("__returns_final",["gvkey","iid","datadate",
        #                                   "ret","ret_local","ret_lag_dif"])
        # util_funcs.delete_table(["__returns","__returns_temp"])
        # util_funcs.rename_table([["__returns_final","__returns"]])
    #     # #query29: create table sec_info
    #     #           union comp_security and comp_g_security
        #executor.execute_and_commit(queries["query29"])
    #     # #query30: create table __delist1_temp
    #     #           select from __returns, create row_number
        #executor.execute_and_commit(queries["query30"])
    #     # #query31: create table __delist1
    #     #           only keep the last observation in each group
        #executor.execute_and_commit(queries["query31"])
    #     # #query70: create table __delist2
    #     #           join __delist1 and __sec_info
        #executor.execute_and_commit(queries["query70"])
    #     # #query75: create table __delist3
    #     #           select from __delist2
        #executor.execute_and_commit(queries["query75"])
    #     # #query80: create table __comp_sf2
    #     #           join base,__returns,__delist3
        executor.execute_and_commit(queries["query80"])
    #     # #query85: create table __comp_sf3
    #     #           select non-delisted observations from __comp_sf2
        executor.execute_and_commit(queries["query85"])
        # #query86: update table __comp_sf3
    #     #           set variable ret
        executor.execute_and_commit(queries["query86"])
    #     # #query87: update table __comp_sf3
    #     #           set variable ret_local
    #    executor.execute_and_commit(queries["query87"])
    #     # #query88: clean up
    #     util_funcs.delete_column([["__comp_sf3","ri"],["__comp_sf3","ri_local"],
    #                               ["__comp_sf3","date_delist"],["__comp_sf3","dlret"]])
    #     if freq_use=="d":
    #         scale=21
    #     elif freq_use=="m":
    #         scale=1
    #     # #query90: create table __comp_sf4
    #     #           join table __comp_sf3, crsp_mcti, ff_factors_monthly
    #     executor.execute_and_commit(queries["query90"].format(scale))
    #
    #     # #query100: create table __comp_sf5
    #     #            join table __comp_sf4, __exchanges
    #     executor.execute_and_commit(queries["query100"])
    #     util_funcs.sort_and_remove_duplicates(table_in="__comp_sf6",table_out=out,idvar="gvkey,iid,datadate",sortvar="gvkey,iid,datadate")
    # # #query101: clean up
    # util_funcs.delete_table(["__firm_shares1","__firm_shares2","fx",
    #                          "__comp_dsf_na","__comp_dsf_global","__comp_dsf1",
    #                          "__comp_dsf2","__comp_dsf3","__returns",
    #                          "__sec_info","__delist1","__delist2",
    #                          "__delist3","__comp_sf1","__comp_sf2",
    #                          "__comp_sf3","__comp_sf4","__comp_sf5",
    #                          "__comp_sf6","__exchanges"])
    #
