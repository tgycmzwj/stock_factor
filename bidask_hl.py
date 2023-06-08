import time
import utils
from query_storage import query_storage
from chars_config import chars_config


def bidask_hl(conn, cursor, out, data, __min_obs):
    # initiate utilities
    queries = query_storage.query_bank["bidask_hl"]
    chars = chars_config()
    util_funcs = utils.utils(conn, cursor)
    executor = utils.executor(conn, cursor)
    print("Starting process bidask_hl at time " + time.asctime())

    executor.execute_and_commit(queries["query1"].format(data=data))
    executor.execute_and_commit(queries["query3"])
    util_funcs.delete_column([["__dsf1","prc_low_r"],["__dsf1","prc_high_r"]])
    executor.execute_and_commit(queries["query7"])
    executor.execute_and_commit(queries["query8"])
    executor.execute_and_commit(queries["query9"])
    executor.execute_and_commit(queries["query10"].format(__min_obs=__min_obs,out=out))
    util_funcs.delete_table(["__dsf1","__dsf2","__dsf3","__dsf4"])

