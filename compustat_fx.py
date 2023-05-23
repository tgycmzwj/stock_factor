import time
import utils
from query_storage import query_storage

def compustat_fx(conn,cursor):
    queries=query_storage.query_bank["compustat_fx"]
    print("Starting processing compustat_fx at time " + time.asctime())
    #query1:
    cursor.execute(queries["query1"])
    cursor.fetchall()
    print("finished query 1 at time "+time.asctime())
    #query2:
    cursor.execute(queries["query2"])
    cursor.fetchall()
    print("finished query 2 at time "+time.asctime())
    #query3:
    cursor.execute(queries["query3"])
    cursor.fetchall()
    print("finished query 3 at time "+time.asctime())
    #query4:
    cursor.execute(queries["query4"])
    cursor.fetchall()
    print("finished query 4 at time "+time.asctime())
    #query5:
    cursor.execute(queries["query5"])
    cursor.fetchall()
    print("finished query 5 at time "+time.asctime())
    #query6:
    cursor.execute(queries["query6"])
    cursor.fetchall()
    print("finished query 6 at time "+time.asctime())
    #query7:
    cursor.execute(queries["query7"])
    cursor.fetchall()
    print("finished query 7 at time "+time.asctime())
    utils.utils.delete_column([["__fx3","datadate"],["__fx3","following"],["__fx3","n"]])
    #query9:
    cursor.execute(queries["query9"])
    cursor.fetchall()
    print("finished query 9 at time "+time.asctime())
    utils.utils.delete_table(["__fx1","__fx2","__fx3"])
