import numpy as np
import pandas as pd
import wrds
import os
os.environ["SQLITE_TMPDIR"]="/kellogg/proj/wzi1638/stock_factor/temp"
import sqlite3
from macro_config import macro_config
from pull_raw_wrds import pull_raw_wrds
from query_storage import query_storage
from prepare_crsp_sf import prepare_crsp_sf


#establish connection to sql
config=macro_config()
db=wrds.Connection(wrds_username=config.wrds_username)
#db.create_pgpass_file()
conn=sqlite3.connect(config.db)
cursor=conn.cursor()
query_bank=query_storage()
queries=query_bank.query_bank["prepare_crsp_sf"]


#download data
# pull_raw_wrds(config.datasets,db,conn,cursor)

#process us data from crsp
# prepare_crsp_sf(conn,cursor,queries,"m")
prepare_crsp_sf(conn,cursor,queries,"d")


print("finished")
