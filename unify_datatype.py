import time
import utils

def unify_datatype(conn,cursor):
    util_funcs=utils.utils(conn,cursor)
    all_tables=util_funcs.list_table()

    for table_name in all_tables:
        all_columns=util_funcs.list_column(table_name=table_name)
        # gvkey to integer
        if "gvkey" in all_columns:
            util_funcs.change_column_type(table_name=table_name,column_name="gvkey",column_type="Integer")
        # permno to integer
        if "permno" in all_columns:
            util_funcs.change_column_type(table_name=table_name,column_name="permno",column_type="Integer")
        # permco to integer
        if "permco" in all_columns:
            util_funcs.change_column_type(table_name=table_name,column_name="permco",column_type="Integer")
        # date to string
        if "datadate" in all_columns:
            util_funcs.change_column_type(table_name=table_name,column_name="datadate",column_type="TEXT")

