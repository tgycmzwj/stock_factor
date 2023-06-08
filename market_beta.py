import time
from query_storage import query_storage
import utils
from chars_config import chars_config
from winsorize_own import winsorize_own

def market_beta(conn, cursor, out, data, fcts, __n, __min):
    # initiate utilities
    queries = query_storage.query_bank["market_beta"]
    chars = chars_config()
    util_funcs = utils.utils(conn, cursor)
    executor = utils.executor(conn, cursor)
    print("Starting process market_beta at time " + time.asctime())

    executor.execute_and_commit(queries["query1"].format(data=data,fcts=fcts))
    winsorize_own(conn,cursor,table_in="__msf1",table_out="__msf2",sortvar="eom",vars="ret_exc",perc_low=0.1,perc_high=99.9)
    util_funcs.sort_table(table="__msf2",sortvar="id,eom")
    executor.execute_and_commit(queries["query2"])
    executor.execute_and_commit(queries["query3"].format(__n=__n))

    for __grp in range(__n):
        executor.execute_and_commit(queries["query4"].format(__grp=__grp))
        executor.execute_and_commit(queries["query5"].format(__min=__min))
        executor.execute_and_commit(queries["query6"])
        executor.execute_and_commit(queries["query7"].format(__n=__n,__min=__min))
        if __grp==0:
            util_funcs.rename_table([["__capm2","op_capm"]])
        else:
            util_funcs.union_table(table1="op_capm",table2="__capm2",table_out="op_capm")

    util_funcs.sort_and_remove_duplicates(table_in="op_capm",table_out=out,sortvar="id,eom",idvar="id,eom")
    util_funcs.delete_table(["op_capm"])

