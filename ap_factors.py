import time
from query_storage import query_storage
import utils
from chars_config import chars_config
from winsorize_own import winsorize_own

def ap_factors(conn,cursor,out,freq,sf,mchars,mkt,min_stocks_bp,min_stocks_pf):
    # initiate utilities
    queries = query_storage.query_bank["ap_factors"]
    chars = chars_config()
    util_funcs = utils.utils(conn, cursor)
    executor = utils.executor(conn, cursor)
    print("Starting process ap_factors at time " + time.asctime())

    if freq=="d":
        executor.execute_and_commit(queries["query1"].format(sf=sf))
        winsorize_own(conn,cursor,table_in="world_sf1",table_out="world_sf2",sortvar="eom",vars="ret_exc",perc_low=0.1,perc_high=99.9)
        __date_col="date"
    if freq=="m":
        executor.execute_and_commit(queries["query2"].format(sf=sf))
        winsorize_own(conn,cursor,table_in="world_sf1",table_out="world_sf2",sortvar="eom",vars="ret_exc",perc_low=0.1,perc_high=99.9)
        __date_col="eom"
    executor.execute_and_commit(queries["query3"].format(mchars=mchars))
    util_funcs.sort_table(table="base1",sortvar="id,eom")

    #add temp
    new_cols=[item+"_l" for item in chars.cols_lag]
    executor.execute_and_commit(queries["query5"].format(new_cols=",".join(new_cols)))
    for col in chars.cols_lag:
        executor.execute_and_commit(queries["query6"].format(col=col))
    util_funcs.delete_column([["base3",col] for col in chars.cols_lag])
    executor.execute_and_commit(queries["query7"])










