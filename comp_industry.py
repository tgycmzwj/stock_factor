import time
import utils
from query_storage import query_storage
from chars_config import chars_config
from hgics_join import hgics_join
from comp_sic_naics import comp_sic_naics

def comp_industry(conn,cursor,out):
    # initiate utilities
    queries = query_storage.query_bank["comp_industry"]
    util_funcs = utils.utils(conn, cursor)
    executor=utils.executor(conn,cursor)
    char_collections=chars_config()
    print("Starting process comp_industry at time " + time.asctime())

    hgics_join(conn,cursor,"comp_gics")
    comp_sic_naics(conn,cursor,"comp_other")

    # query1: create table join1
    #         join comp_gics and comp_other
    executor.execute_and_commit(queries["query1"])

    # query2: create table join2
    #         select from join1
    executor.execute_and_commit(queries["query2"])

    # query3: update table join2
    #         set gap
    executor.execute_and_commit(queries["query3"])

    # query4: create table gap1
    #        select from join2
    executor.execute_and_commit(queries["query4"])
    util_funcs.duplicate_records(table_in="gap2",table_out="gap3",num="diff")
    util_funcs.delete_column([["gap3","lagdate"],["gap3","date_1"],
                              ["gap3","gap"],["gap3","diff"],
                              ["gap3","n"]])

    # query5: create table joined1
    #         join join1 and gap3
    executor.execute_and_commit(queries["query5"])
    util_funcs.sort_and_remove_duplicates(table_in="joined1",table_out=out,sortvar="gvkey,date",idvar="gvkey,date")
    util_funcs.delete_table(["comp_gics","comp_other","join1","join2","join3","gap1","gap2","gap3","joined1"])

