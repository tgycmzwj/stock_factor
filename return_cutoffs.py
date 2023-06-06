import time
import utils
from query_storage import query_storage
from chars_config import chars_config
from hgics_join import hgics_join
from comp_sic_naics import comp_sic_naics

def return_cutoffs(conn,cursor,data,freq,out,crsp_only):
    # initiate utilities
    queries = query_storage.query_bank["return_cutoffs"]
    util_funcs = utils.utils(conn, cursor)
    executor=utils.executor(conn,cursor)
    char_collections=chars_config()
    print("Starting process return_cutoffs at time " + time.asctime())

    date_var="eom" if freq=="m" else "date"
    by_vars="eom" if freq=="m" else "year,month"

    if crsp_only==1:
        executor.execute_and_commit(queries["query1"].format(data=data,date_var=date_var))
    elif crsp_only==0:
        executor.execute_and_commit(queries["query2"].format(data=data,date_var=date_var))
    ret_types=["ret","ret_local","ret_exc"]
    for i in range(len(ret_types)):
        ret_type=ret_types[i]
        executor.execute_and_commit(queries["query3"].format(by_vars=by_vars,ret_type=ret_type))
        if i==0:
            executor.execute_and_commit(queries["query4"].format(out=out))
        elif freq=="m":
            executor.execute_and_commit(queries["query5"].format(out=out,ret_type=ret_type))
        elif freq=="d":
            executor.execute_and_commit(queries["query6"].format(out=out,ret_type=ret_type))
    util_funcs.delete_table(["cutoffs","base"])


