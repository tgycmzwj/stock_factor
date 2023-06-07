#external dependencies
import numpy as np
import pandas as pd
#import wrds
import os
os.environ["SQLITE_TMPDIR"]="/kellogg/proj/wzi1638/stock_factor/temp"
import sqlite3

#project dependencies
import utils
from ap_factors import ap_factors
from macro_config import macro_config
from chars_config import chars_config
from pull_raw_wrds import pull_raw_wrds
from prepare_crsp_sf import prepare_crsp_sf
from prepare_comp_sf import prepare_comp_sf
from helper_func import prepare_helper_func
from unify_datatype import unify_datatype
from compustat_fx import compustat_fx
from combine_crsp_comp_sf import combine_crsp_comp_sf
from crsp_industry import crsp_industry
from comp_industry import comp_industry
from market_returns import market_returns
from query_storage import query_storage
from ff_ind_class import ff_ind_class
from nyse_size_cutoffs import nyse_size_cutoffs
from return_cutoffs import return_cutoffs
from standardized_accounting_data import standardized_accounting_data
from combine_ann_qtr_chars import combine_ann_qtr_chars
from firm_age import firm_age

class main():
    def run_all_procedures(self):
        #obtain configuration for the program
        config=macro_config()
        chars=chars_config()

        ##connect to wrds terminal
        #db=wrds.Connection(wrds_username=config.wrds_username)
        #db.create_pgpass_file()

        #connect to local sqlite database
        conn=sqlite3.connect(config.db)
        conn.enable_load_extension(True)
        conn.load_extension("./sqlean-linux-x86/stats")
        cursor=conn.cursor()
        util_funcs = utils.utils(conn, cursor)
        executor = utils.executor(conn, cursor)
        queries = query_storage.query_bank["ff_ind_class"]

        # #download data
        # pull_raw_wrds(config.datasets,db,conn,cursor)

        # #unify data type
        # unify_datatype(conn,cursor)

        # #define additional helper function
        prepare_helper_func(conn)

        # #process us data from crsp
        # prepare_crsp_sf(conn,cursor,"m")
        # prepare_crsp_sf(conn,cursor,"d")

        # #process world data from compustat
        prepare_comp_sf(conn,cursor,"m")

        #merge compustat and crsp data together
        combine_crsp_comp_sf(conn,cursor,out_msf="world_msf1",out_dsf="world_dsf",
                             crsp_msf="crsp_msf",comp_msf="comp_msf",crsp_dsf="crsp_dsf",comp_dsf="comp_dsf")
        util_funcs.delete_table(["comp_dsf","crsp_dsf","comp_msf","crsp_msf"])

        #add industry code for crsp
        crsp_industry(conn,cursor,"crsp_ind")
        comp_industry(conn,cursor,"comp_ind")

        #create table world_msf2
        executor.execute_and_commit(queries["query1"])
        util_funcs.delete_table(["world_msf1","crsp_ind","comp_ind"])

        #add a column ff49 with Fama-French industry classification
        ff_ind_class(conn,cursor,49,"world_msf3")

        #size cutoff
        nyse_size_cutoffs(conn,cursor,"world_msf3","nyse_cutoffs")

        #classify stocks into size groups
        executor.execute_and_commit(queries["query2"])
        util_funcs.delete_table(["world_msf2","world_msf3"])

        #return cutoffs
        return_cutoffs(conn,cursor,data="world_msf",freq="m",out="return_cutoffs",crsp_only=0)
        return_cutoffs(conn,cursor,data="world_dsf",freq="d",out="return_cutoffs_daily",crsp_only=0)

        #market returns
        market_returns(conn,cursor,out="market_returns",data="world_msf",freq="m",wins_comp=1,wins_data="return_cutoffs")
        market_returns(conn,cursor,out="market_returns_daily",data="world_dsf",freq="d",wins_comp=1,wins_data="return_cutoffs,daily")

        #create characteristics based on accounting data
        standardized_accounting_data(conn,cursor,coverage="world",convert_to_usd=1,me_data="world_msf",include_helpers_vars=1,start_date="1949-12-31")

        combine_ann_qtr_chars(out="acc_chars_world",ann_data="achars_world",qtr_data="qchars_world",__char_vars=chars.acc_chars,q_suffix="_qitem")





        #combine returns, accounting and monthly market data
        executor.execute_and_commit(queries["query3"])
        util_funcs.delete_column([["world_data_prelim","div_tot"],
                                  ["world_data_prelim","div_cash"],
                                  ["world_data_prelim","div_spc"],
                                  ["world_data_prelim","public_date"],
                                  ["world_data_prelim","source"]])

        #asset pricing factors
        #create monthly and daily factors from ff3 and hxz4
        ap_factors(conn,cursor,out="ap_factors_daily",freq="d",sf="world_dsf",mchars="world_data_prelim",mkt="market_returns_daily",min_stocks_bp=10,min_stocks_pf=3)
        ap_factors(conn,cursor,out="ap_factors_monthly",freq="m",sf="world_msf",mchars="world_data_prelim",mkt="market_returns",min_stocks_bp=10,min_stocks_pf=3)

        #factor based on combined data
        firm_age(conn,cursor,data="world_msf",out="firm_age")





if __name__=="__main__":
    main_program=main()
    main_program.run_all_procedures()
    print("finished")
