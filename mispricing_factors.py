import time
from query_storage import query_storage
import utils
from chars_config import chars_config


def mispricing_factors(conn, cursor, out, data, min_stks, min_fcts=3):
    # initiate utilities
    queries = query_storage.query_bank["mispricing_factors"]
    chars = chars_config()
    util_funcs = utils.utils(conn, cursor)
    executor = utils.executor(conn, cursor)
    print("Starting process mispricing_factors at time " + time.asctime())

    executor.execute_and_commit(queries["query1"].format(data=data))
    __vars=["chcsho_12m","eqnpo_12m","oaccruals_at","noa_at","at_gr1","ppeinv_gr1a","o_score","ret_12_1","gp_at niq_at"]
    __direction=[-1,1,-1,-1,-1,-1,-1,1,1,1]
    for i in range(10):
        __v=__vars[i]
        __d=__direction[i]
        if __d==1:
            __sort=""
        else:
            __sort="descending"
        executor.execute_and_commit(queries["query2"].format(__v=__v,min_stks=min_stks))
        executor.execute_and_commit(queries["query3"].format(__v=__v,__sort=__sort))
        executor.execute_and_commit(queries["query4"].format(nums1=i+2,nums2=i+1,__v=__v))
        executor.execute_and_commit(queries["query5"].format(out=out,min_fcts=min_fcts))
