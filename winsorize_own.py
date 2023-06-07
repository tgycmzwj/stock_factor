import time
from query_storage import query_storage
import utils
from chars_config import chars_config


def winsorize_own(conn, cursor, table_in, table_out, sortvar, vars, perc_low, perc_high):
    # initiate utilities
    queries = query_storage.query_bank["winsorize_own"]
    chars = chars_config()
    util_funcs = utils.utils(conn, cursor)
    executor = utils.executor(conn, cursor)
    print("Starting process winsorize_own at time " + time.asctime())
