import time
from query_storage import query_storage
import utils

def market_returns(conn,cursor,out,data,freq="m",wins_comp=1,wins_data="return_cutoffs"):
    # initiate utilities
    queries = query_storage.query_bank["market_returns"]
    util_funcs = utils.utils(conn, cursor)
    executor = utils.executor(conn, cursor)
    print("Starting process market_returns at time " + time.asctime())

    dt_col="date" if freq=="d" else "eom"
    max_date_lag=14 if freq=="d" else 1

    executor.execute_and_commit(queries["query1"].format(data=data,dt_col=dt_col))
    executor.execute_and_commit(queries["query2"])

    if wins_comp==1:
        if freq=="m":
            executor.execute_and_commit(queries["query3"].format(wins_data=wins_data))
        elif freq=="d":
            executor.execute_and_commit(queries["query4"].format(wins_data=wins_data))
        executor.execute_and_commit(queries["query5"].format(var="ret"))
        executor.execute_and_commit(queries["query5"].format(var="ret_local"))
        executor.execute_and_commit(queries["query5"].format(var="ret_exc"))
        util_funcs.delete_column([["__common_stocks3","ret_exc_0_1"],
                                  ["__common_stocks3","ret_exc_99_9"],
                                  ["__common_stocks3","ret_0_1"],
                                  ["__common_stocks3","ret_99_9"],
                                  ["__common_stocks3","ret_local_0_1"],
                                  ["__common_stocks3","ret_local_99_9"]])
    elif wins_comp==0:
        util_funcs.rename_table([["__common_stocks2","__common_stocks3"]])
    executor.execute_and_commit(queries["query9"].format(dt_col=dt_col,max_date_lag=max_date_lag))
    if freq=="m":
        util_funcs.rename_table([["mkt1",out]])
    elif freq=="d":
        executor.execute_and_commit(queries["query10"].format(out=out))
    util_funcs.delete_table(["__common_stocks1","__common_stocks2","__common_stocks3","mkt1"])


