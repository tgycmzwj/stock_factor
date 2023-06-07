import time
from query_storage import query_storage
import utils
from chars_config import chars_config


def market_beta(conn, cursor, out, data, fcts, __n, __min):
    # initiate utilities
    queries = query_storage.query_bank["market_beta"]
    chars = chars_config()
    util_funcs = utils.utils(conn, cursor)
    executor = utils.executor(conn, cursor)
    print("Starting process market_beta at time " + time.asctime())