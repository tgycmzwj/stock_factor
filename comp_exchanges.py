
import time
import utils
from query_storage import query_storage
from chars_config import chars_config

def comp_exchanges(conn,cursor,out):
    # initiate utilities
    queries = query_storage.query_bank["comp_exchanges"]
    util_funcs = utils.utils(conn, cursor)
    executor=utils.executor(conn,cursor)
    char_collections=chars_config()
    print("Starting process comp_exchanges at time " + time.asctime())

    #query1: create table __ex_country1
    #        union table comp_g_security and comp_security
    executor.execute_and_commit(queries["query1"])

    #query2: create table __ex_country2
    #        select from __ex_country1
    executor.execute_and_commit(queries["query2"])

    #query3: create table __ex_country3
    #        join table __ex_country2 and comp_r_ex_codes
    executor.execute_and_commit(queries["query3"])

    #query4: create table {out}
    #        select from __ex_country3
    executor.execute_and_commit(queries["query4"].format(out=out))
    #clean up
    util_funcs.delete_table(["__ex_country1","__ex_country2",
                             "__ex_country3"])






