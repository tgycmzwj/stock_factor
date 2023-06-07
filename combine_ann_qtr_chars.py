import time
from query_storage import query_storage
import utils
from chars_config import chars_config

def combine_ann_qtr_chars(conn,cursor,out,ann_data,qtr_data,__char_vars,q_suffix):
    # initiate utilities
    queries = query_storage.query_bank["combine_ann_qtr_chars"]
    chars = chars_config()
    util_funcs = utils.utils(conn, cursor)
    executor = utils.executor(conn, cursor)
    print("Starting process combine_ann_qtr_chars at time " + time.asctime())

    executor.execute_and_commit(queries["query1"].format(ann_data=ann_data,qtr_data=qtr_data))
    for i in range(len(__char_vars)):
        ann_var=__char_vars[i]
        qtr_var=ann_var+q_suffix
        executor.execute_and_commit(queries["query2"].format(ann_var=ann_var,qtr_var=qtr_var,q_suffix=q_suffix))
    util_funcs.sort_and_remove_duplicates(table_in="__acc_chars1",table_out=out,sortvar="gvkey,public_date",idvar="gvkey,public_date")
    util_funcs.delete_table(["__acc_chars1"])
