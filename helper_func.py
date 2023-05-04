import datetime
from dateutil.relativedelta import relativedelta
import calendar

def intnx_(date,n,delta,alignment):
    if delta=="month":
        date_after=datetime.datetime.strptime(date,"%Y-%m-%d")+relativedelta(months=n)
    elif delta=="day":
        date_after=datetime.datetime.strptime(date,"%Y-%m-%d")+relativedelta(days=n)
    if alignment=="end":
        day=calendar.monthrange(date_after.year,date_after.month)[1]
        return datetime.datetime(date_after.year,date_after.month,day).strftime("%Y-%m-%d")
    else:
        return date_after.strftime("%Y-%m-%d")
def prepare_helper_func(conn):
    conn.create_function("INTNX_",4,intnx_)
    print("finished creating helper functions")
