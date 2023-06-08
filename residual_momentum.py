import time
import utils

from query_storage import query_storage
from chars_config import chars_config
from winsorize_own import winsorize_own

def residual_momentum(conn, cursor, out, data, fcts, type, __n, __min, incl, skip):
    # initiate utilities
    queries = query_storage.query_bank["residual_momentum"]
    chars = chars_config()
    util_funcs = utils.utils(conn, cursor)
    executor = utils.executor(conn, cursor)
    print("Starting process residual_momentum at time " + time.asctime())

    executor.execute_and_commit(queries["query1"].format(data=data,fcts=fcts))
    winsorize_own(conn,cursor,table_in="__msf1",table_out="__msf2",sortvar="eom",vars="ret_exc",perc_low=0.1,perc_high=99.9)
    util_funcs.sort_table(table="__msf2",sortvar="id,eom")
    executor.execute_and_commit(queries["query2"])
    executor.execute_and_commit(queries["query3"].format(__n=__n))
    for __grp in range(__n):
        executor.execute_and_commit(queries["query4"].format(__n=__n,__grp=__grp))
        executor.execute_and_commit(queries["query5"].format(__min=__min))
        if "ff3" in type:
            executor.execute_and_commit(queries["query6"])
            intercept,mktrf_coeff,smb_ff_coeff,hml_coeff=0,0,0,0
            executor.execute_and_commit(queries["query7"].format(intercept=intercept,mktrf_coeff=mktrf_coeff,smb_ff_coeff=smb_ff_coeff,hml_coeff=hml_coeff))
            for i in range(len(incl)):
                __in=incl[i]
                __sk=skip[i]
                executor.execute_and_commit(queries["query8"].format(__in=__in,__sk=__sk,__min=__min))
                executor.execute_and_commit(queries["query9"])
                if __grp==0:
                    util_funcs.rename_table([["__ff3_res3", "op_"+__in+"_"+__sk]])
                else:
                    util_funcs.union_table(table1="op_"+__in+"_"+__sk, table2="__ff3_res3", table_out="op_"+__in+"_"+__sk)
    for i in range(len(incl)):
        __in=incl[i]
        __sk=skip[i]
        util_funcs.sort_and_remove_duplicates(table_in="op_"+__in+"_"+__sk,table_out=out+"_"+__in+"_"+__sk,sortvar="id,eom",idvar="id,eom")
        util_funcs.delete_table(["op_"+__in+"_"+__sk])
