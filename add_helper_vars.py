import time
from query_storage import query_storage
import utils
from chars_config import chars_config

def add_helper_vars(conn,cursor,data,out):
    # initiate utilities
    queries = query_storage.query_bank["standardized_accounting_data"]
    chars = chars_config()
    util_funcs = utils.utils(conn, cursor)
    executor = utils.executor(conn, cursor)
    print("Starting process add_helper_vars at time " + time.asctime())

    executor.execute_and_commit(queries["query1"].format(data=data))
