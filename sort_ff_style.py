import time
from query_storage import query_storage
import utils
from chars_config import chars_config

def sort_ff_style(conn,cursor,out,char,freq,min_stocks_bp,min_stocks_pf):
    # initiate utilities
    queries = query_storage.query_bank["sort_ff_style"]
    chars = chars_config()
    util_funcs = utils.utils(conn, cursor)
    executor = utils.executor(conn, cursor)
    print("Starting process sort_ff_style at time " + time.asctime())