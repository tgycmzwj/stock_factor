#external dependencies
import numpy as np
import pandas as pd
import wrds
import os
os.environ["SQLITE_TMPDIR"]="/kellogg/proj/wzi1638/stock_factor/temp"
import sqlite3

#project dependencies
from macro_config import macro_config
from pull_raw_wrds import pull_raw_wrds
from query_storage import query_storage
from prepare_crsp_sf import prepare_crsp_sf
from prepare_comp_sf import prepare_comp_sf
from helper_func import prepare_helper_func

#establish connection to sql
config=macro_config()
db=wrds.Connection(wrds_username=config.wrds_username)
#db.create_pgpass_file()
conn=sqlite3.connect(config.db)
cursor=conn.cursor()
query_bank=query_storage()


# #download data
# pull_raw_wrds(config.datasets,db,conn,cursor)

#define additional helper function


# #process us data from crsp
# queries=query_bank.query_bank["prepare_crsp_sf"]
# prepare_crsp_sf(conn,cursor,queries,"m")
# prepare_crsp_sf(conn,cursor,queries,"d")

#process world data from compustat
queries=query_bank.query_bank["prepare_comp_sf"]
prepare_comp_sf(conn,cursor,queries,"m")

print("finished")
