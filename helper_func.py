import datetime
from dateutil.relativedelta import relativedelta
import calendar
import typing

def intnx_(date:str,n:int,delta:str,alignment:str)->str:
    try:
        if delta.lower()=="month":
            date_after=datetime.datetime.strptime(date,"%Y-%m-%d")+relativedelta(months=n)
            if alignment.lower() == "end":
                day = calendar.monthrange(date_after.year, date_after.month)[1]
                return datetime.datetime(date_after.year, date_after.month, day).strftime("%Y-%m-%d")
            return date_after.strftime("%Y-%m-%d")
        elif delta.lower()=="day":
            date_after=datetime.datetime.strptime(date,"%Y-%m-%d")+relativedelta(days=n)
            return date_after.strftime("%Y-%m-%d")
    except:
        return "null"


def prepare_helper_func(conn):
    conn.create_function("INTNX_",4,intnx_)
    print("finished creating helper functions")
