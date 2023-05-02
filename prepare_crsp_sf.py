import time
import utils

def prepare_crsp_sf(conn,cursor,queries,freq="m"):
    print("Starting processing freq={freq} at time ".format(freq=freq) + time.asctime())
    #query1
    cursor.execute(queries["query1"].format(freq=freq))
    cursor.fetchall()
    print("finished query 1 at time "+time.asctime())

    #query2
    cursor.execute(queries["query2"])
    cursor.fetchall()
    conn.commit()
    print("finished query 2 at time "+time.asctime())

    #query3
    cursor.execute(queries["query3"])
    cursor.fetchall()
    print("finished query 3 at time "+time.asctime())

    #query4
    cursor.execute(queries["query4"])
    cursor.fetchall()
    print("finished query 4 at time "+time.asctime())

    #query5
    if freq=="m":
        cursor.execute(queries["query5_m"].format(freq=freq))
        cursor.fetchall()
    else:
        cursor.execute(queries["query5_d"].format(freq=freq))
        cursor.fetchall()
    print("finished query 5 at time "+time.asctime())

    #query6
    cursor.execute(queries["query6"])
    cursor.fetchall()
    print("finished query 6 at time "+time.asctime())
    for i in range(6):
        cursor.execute(queries["query6_"+str(i)])
        cursor.fetchall()
        print("finished query6_"+str(i)+" at time "+time.asctime())
        conn.commit()

    #query7
    if freq=="m":
        scale="21"
    else:
        scale="1"
    print("start")
    cursor.execute(queries["query7"].format(scale=scale))
    cursor.fetchall()
    print("finished query7 at time "+time.asctime())

    #query8
    cursor.execute(queries["query8"])
    cursor.fetchall()
    print("finished query8 at time "+time.asctime())

    #query9
    if freq=="m":
        cursor.execute(queries["query9"])
        cursor.fetchall()
        print("finished query9 at time "+time.asctime())
        conn.commit()

    #query10
    cursor.execute(queries["query10"].format(freq=freq))
    cursor.fetchall()
    print("finished query10 at time "+time.asctime())

    #query11
    for i in range(1,9):
        cursor.execute(queries["query11_"+str(i)])
        cursor.fetchall()
        print("finished query11_"+str(i)+" at time "+time.asctime())

    print("finished")
