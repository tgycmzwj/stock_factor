import time
import utils
import re

def unify_datatype(conn,cursor):
    util_funcs=utils.utils(conn,cursor)
    all_tables=util_funcs.list_table()

    for table_name in all_tables:
        all_columns=util_funcs.list_column(table_name=table_name)
        for column_name in all_columns:
            # gvkey to integer
            if column_name in ["gvkey","permno","permco"]:
                util_funcs.change_column_type(table_name=table_name,column_name=column_name,column_type="Integer")
            # date to string
            if re.match(".*date$",column_name):
                util_funcs.change_column_type(table_name=table_name,column_name=column_name,column_type="TEXT")

