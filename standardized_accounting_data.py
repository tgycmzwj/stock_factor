import time
from query_storage import query_storage
import utils
from chars_config import chars_config
from compustat_fx import compustat_fx

def standardized_accounting_data(conn,cursor,coverage,convert_to_usd,me_data,include_helpers_vars=1,start_date="1950-01-01"):
    # initiate utilities
    queries = query_storage.query_bank["standardized_accounting_data"]
    chars=chars_config()
    util_funcs = utils.utils(conn, cursor)
    executor = utils.executor(conn, cursor)
    print("Starting process standardized_accounting_data at time " + time.asctime())

    qvars_q=executor.execute_and_commit(queries["query1"])
    qvars_y=executor.execute_and_commit(queries["query2"])
    qvars=list(qvars_q)+list(qvars_y)
    if coverage=="global" or coverage=="world":
        aname="__gfunda"
        qname="__gfundq"
        compcond="indfmt in ('INDL', 'FS') and datafmt='HIST_STD' and popsrc='I' and consol='C'"
        executor.execute_and_commit(queries["query3"].format(compcond=compcond,date=start_date,keep_list=chars.avars+chars.avars_other))
        executor.execute_and_commit(queries["query4"].format(aname=aname))
        util_funcs.delete_column([[aname,"indfmt"]])
        executor.execute_and_commit(queries["query6"].format(compcond=compcond))
        util_funcs.keep_column(table="g_fundq1",columns=["gvkey,datadate","indfmt","fyr","fyearq","fqtr","curcdq","source"]+qvars)
        executor.execute_and_commit(queries["query7"].format(qname=qname))
        util_funcs.delete_column([[qname,"indfmt"]])
    if coverage=="na" or coverage=="world":
        aname="__funda"
        qname="__fundq"
        compcond="indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'"
        executor.execute_and_commit(queries["query9"].format(aname=aname,compcond=compcond,start_date=start_date,keep_list=chars.avars+chars.avars_other))
        executor.execute_and_commit(queries["query10"].format(aname=aname,compcond=compcond,start_date=start_date,keep_list=qvars))
    if coverage=="world":
        aname="__wfunda"
        qname="__wfundq"
        executor.execute_and_commit(queries["query10_5"].format(combine=aname,freq="a"))
        executor.execute_and_commit(queries["query10_5"].format(combine=qname,freq="q"))
        util_funcs.delete_table(["__gfunda","__gfundq","__funda","__fundq"])
    if convert_to_usd==1:
        compustat_fx(conn,cursor)
        executor.execute_and_commit(queries["query11"].format(aname=aname))
        executor.execute_and_commit(queries["query12"].format(qname=qname))

    if include_helpers_vars==1:
        qdata="__compq6"
        adata="__compa4"
        util_funcs.delete_table(["__compq5","__compa3"])
    else:
        qdata="__compq5"
        adata="__compa3"
    util_funcs.sort_and_remove_duplicates(table_in=adata,table_out="acc_std_ann",sortvar="gvkey,datadate",idvar="gvkey,datadate")
    util_funcs.sort_and_remove_duplicates(table_in=qdata,table_out="acc_std_qtr",sortvar="gvkey,datadate",idvar="gvkey,datadate")
    util_funcs.delete_table(["__compq1","__compq2","__compq3","__compq4","__compa1","_compa2",qdata,adata])




