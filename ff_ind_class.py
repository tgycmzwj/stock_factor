import time
import utils
from query_storage import query_storage
from chars_config import chars_config

def ff_ind_class(conn,cursor,ff_grps,out):
    # initiate utilities
    queries = query_storage.query_bank["ff_ind_class"]
    util_funcs = utils.utils(conn, cursor)
    executor=utils.executor(conn,cursor)
    char_collections=chars_config()
    print("Starting process ff_ind_class at time " + time.asctime())

    if ff_grps==38:
        executor.execute_and_commit(queries["query1"].format(out))
    elif ff_grps==49:
        executor.execute_and_commit(queries["query2"].format(out))