import time
import utils
from query_storage import query_storage
from chars_config import chars_config
from hgics_join import hgics_join
from comp_sic_naics import comp_sic_naics

def return_cutoffs(conn,cursor,data,freq,out,crsp_only):
    # initiate utilities
    queries = query_storage.query_bank["return_cutoffs"]
    util_funcs = utils.utils(conn, cursor)
    executor=utils.executor(conn,cursor)
    char_collections=chars_config()
    print("Starting process return_cutoffs at time " + time.asctime())

    if freq=="m":
        date_var="eom"
        by_vars="eom"
    elif freq=="d":
        date_var="date"
        by_vars="year,month"

    if crsp_only==1:
        executor.execute_and_commit(queries["query1"].format(data=data,date_var=date_var))
    elif crsp_only==0:
        executor.execute_and_commit(queries["query2"].format(data=data,date_var=date_var))

    if freq=="d":

