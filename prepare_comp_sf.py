import time



def prepare_comp_sf(conn,cursor,queries,freq="m"):
    # print("Starting processing freq={freq} at time ".format(freq=freq) + time.asctime())

    # #query1
    # cursor.execute(queries["query1"].format(freq=freq))
    # cursor.fetchall()
    # print("finished query 1 at time "+time.asctime())

    #query2
    # cursor.execute(queries["query2"])
    # cursor.fetchall()
    # print("finished query 2 at time "+time.asctime())
    # cursor.execute(queries["query2_1"])
    # cursor.fetchall()
    # print("finished query 2_1 at time "+time.asctime())

    #query3
    cursor.execute(queries["query3"])
    cursor.fetchall()
    print("finished query 3 at time "+time.asctime())
    for i in range(5):
        cursor.execute(queries["query3_"+str(i)])
        cursor.fetchall()
        print("finished query 3_{} at time ".format(i)+time.asctime())


