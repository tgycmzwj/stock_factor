import time



def prepare_comp_sf(conn,cursor,queries,freq="m"):
    print("Starting processing freq={freq} at time ".format(freq=freq) + time.asctime())

    #query1
    cursor.execute(queries["query1"].format(freq=freq))
    cursor.fetchall()
    print("finished query 1 at time "+time.asctime())

    #query2
    cursor.execute(queries["query2"])
    cursor.fetchall()
    print("finished query 2 at time "+time.asctime())
    cursor.execute(queries["query2_1"])
    cursor.fetchall()
    print("finished query 2_1 at time "+time.asctime())

    #query3
    cursor.execute(queries["query3_1"])
    cursor.fetchall()
    print("finished query 3_1 at time "+time.asctime())

    cursor.execute(queries["query3_2"])
    cursor.fetchall()
    print("finished query 3_2 at time "+time.asctime())

    cursor.execute(queries["query3_3"])
    cursor.fetchall()
    print("finished query 3_3 at time "+time.asctime())

    #query4
    for i in range(1,10):
        cursor.execute(queries["query4_{}".format(i)])
        cursor.fetchall()
        print("finished query 4_{} at time ".format(i)+time.asctime())



