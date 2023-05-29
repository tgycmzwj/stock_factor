#external dependencies
import numpy as np
import pandas as pd
#import wrds
import os
os.environ["SQLITE_TMPDIR"]="/kellogg/proj/wzi1638/stock_factor/temp"
import sqlite3

#project dependencies
import utils
from macro_config import macro_config
from pull_raw_wrds import pull_raw_wrds
from prepare_crsp_sf import prepare_crsp_sf
from prepare_comp_sf import prepare_comp_sf
from helper_func import prepare_helper_func
from unify_datatype import unify_datatype
from compustat_fx import compustat_fx
from combine_crsp_comp_sf import combine_crsp_comp_sf



class main():
    def run_all_procedures(self):
        #obtain configuration for the program
        config=macro_config()

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

if __name__=="__main__":
    main_program=main()
    main_program.run_all_procedures()
    print("finished")
