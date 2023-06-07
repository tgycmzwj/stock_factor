import time
from query_storage import query_storage
import utils
from chars_config import chars_config

def firm_age(conn,cursor,data,out):
    # initiate utilities
    queries = query_storage.query_bank["firm_age"]
    chars = chars_config()
    util_funcs = utils.utils(conn, cursor)
    executor = utils.executor(conn, cursor)
    print("Starting process firm_age at time " + time.asctime())

    executor.execute_and_commit(queries["query1"])
    executor.execute_and_commit(queries["query2"])
    executor.execute_and_commit(queries["query3"])
    executor.execute_and_commit(queries["query4"])
    executor.execute_and_commit(queries["query5"])
    executor.execute_and_commit(queries["query6"])
    executor.execute_and_commit(queries["query7"])
    executor.execute_and_commit(queries["query8"])
    executor.execute_and_commit(queries["query9"])
    executor.execute_and_commit(queries["query10"])
    util_funcs.delete_column([["comb3","first_obs"],["comb3","first_alt"]])
    executor.execute_and_commit(queries["query12"])



