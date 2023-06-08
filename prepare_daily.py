import time
import utils
from query_storage import query_storage
from chars_config import chars_config
from winsorize_own import winsorize_own

def prepare_daily(conn, cursor, data, fcts):
    # initiate utilities
    queries = query_storage.query_bank["prepare_daily"]
    chars = chars_config()
    util_funcs = utils.utils(conn, cursor)
    executor = utils.executor(conn, cursor)
    print("Starting process prepare_daily at time " + time.asctime())

    executor.execute_and_commit(queries["query1"].format(data=data,fcts=fcts))
    executor.execute_and_commit(queries["query2"])
    util_funcs.delete_column([["dsf1","ret_lag_dif"],["dsf1","bidask"]])
    util_funcs.sort_table(table="dsf1",sortvar="id,date")
    executor.execute_and_commit(queries["query5"].format(fcts=fcts))
    executor.execute_and_commit(queries["query6"])
    util_funcs.sort_table(table="mkt_lead_lag2",sortvar="excntry,date")
    executor.execute_and_commit(queries["query8"])
    executor.execute_and_commit(queries["query9"])
    executor.execute_and_commit(queries["query10"])
