import time
import utils
from query_storage import query_storage
from chars_config import chars_config


def combine_crsp_comp_sf(conn,cursor,out_msf,out_dsf,crsp_msf,comp_msf,crsp_dsf,comp_dsf):
    # initiate utilities
    queries = query_storage.query_bank["combine_crsp_comp_sf"]
    util_funcs = utils.utils(conn, cursor)
    executor = utils.executor(conn, cursor)
    char_collections = chars_config()
    print("Starting process combine_crsp_comp_sf at time " + time.asctime())

    # query1: create table __msf_world1
    #         union {crsp_msf} and {comp_msf}
    executor.execute_and_commit(queries["query1"])
    util_funcs.sort_and_remove_duplicates(table_in="__msf_world1",table_out="__msf_world1",sortvar="id,eom desc",idvar="id,eom")

    # query2: create table __msf_world2
    #         select from __msf_world1
    executor.execute_and_commit(queries["query2"])

    # query3: update table __msf_world2
    #         set column ret_exc_lead1m
    executor.execute_and_commit(queries["query3"])

    # query4: create table __dsf_world1
    #         union {crsp_dsf} and {comp_dsf}
    executor.execute_and_commit(queries["query4"].format(crsp_dsf=crsp_dsf,comp_dsf=comp_dsf))

    # query5: create table __obs_main
    #         select from __msf_world2
    executor.execute_and_commit(queries["query5"])

    # query6: create table __msf_world3
    #         join __msf_world2 and __obs_main
    executor.execute_and_commit(queries["query6"])

    # query7: create table __obs_main
    #         select from __dsf_world2
    executor.execute_and_commit(queries["query7"])
    #clean up
    util_funcs.sort_and_remove_duplicates(table_in="__msf_world3",table_out=out_msf,sortvar="id,eom",idvar="id,eom")
    util_funcs.sort_and_remove_duplicates(table_in="__dsf_world2",table_out=out_dsf,sortvar="id,date",idvar="id,date")
    util_funcs.delete_table(["__msf_world1","__msf_world2","__msf_world3",
                             "__dsf_world1","__dsf_world2","__obs_main"])














