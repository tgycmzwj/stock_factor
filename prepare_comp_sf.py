import time

def prepare_comp_sf(conn,cursor,queries,freq="m"):
    print("Starting processing freq={freq} at time ".format(freq=freq) + time.asctime())
    #query1
    cursor.execute(queries["query1"].format(freq=freq))
    cursor.fetchall()
    print("finished query 1 at time "+time.asctime())
