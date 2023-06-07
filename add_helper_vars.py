import time
from query_storage import query_storage
import utils
from chars_config import chars_config

def add_helper_vars(conn,cursor,data,out):
    # initiate utilities
    queries = query_storage.query_bank["add_helper_vars"]
    chars = chars_config()
    util_funcs = utils.utils(conn, cursor)
    executor = utils.executor(conn, cursor)
    print("Starting process add_helper_vars at time " + time.asctime())

    executor.execute_and_commit(queries["query1"].format(data=data))
    util_funcs.expand_records(table_in="__comp_dates1",table_out="__comp_dates2",id_vars="gvkey",
                              start_date="start_date",end_date="end_date",freq="month",new_date_name="datadate")

    executor.execute_and_commit(queries["query2"].format(data=data))
    util_funcs.sort_and_remove_duplicates(table_in="__helpers1",table_out="__helpers2",sortvar="gvkey,curcd,datadate",idvar="gvkey,curcd,datadate")
    executor.execute_and_commit(queries["query4"])
    for var_pos in ["sale","revt","dv","che"]:
        executor.execute_and_commit(queries["query5"].format(variable=var_pos))
    executor.execute_and_commit(queries["query6"])
    for var_pos in ["be_x","bev_x"]:
        executor.execute_and_commit(queries["query5"].format(variable=var_pos))
    util_funcs.delete_table(["__comp_dates1","__comp_dates2","__helpers1","__helpers2"])

