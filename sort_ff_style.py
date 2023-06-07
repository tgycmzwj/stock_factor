import time
from query_storage import query_storage
import utils
from chars_config import chars_config

def sort_ff_style(conn,cursor,out,freq,sf,mchars,mkt,min_stocks_bp,min_stocks_pf):
    # initiate utilities
    queries = query_storage.query_bank["ap_factors"]
    chars = chars_config()
    util_funcs = utils.utils(conn, cursor)
    executor = utils.executor(conn, cursor)
    print("Starting process ap_factors at time " + time.asctime())