import datetime
from dateutil.relativedelta import relativedelta
import calendar
import typing
import math

def intnx_(date:str,n:int,delta:str,alignment:str)->str:
    try:
        if delta.lower()=="month":
            date_after=datetime.datetime.strptime(date,"%Y-%m-%d")+relativedelta(months=n)
            if alignment.lower() == "end":
                day = calendar.monthrange(date_after.year, date_after.month)[1]
                return datetime.datetime(date_after.year, date_after.month, day).strftime("%Y-%m-%d")
            return date_after.strftime("%Y-%m-%d")
        elif delta.lower()=="year":
            date_after=datetime.datetime.strptime(date,"%Y-%m-%d")+relativedelta(years=n)
            if alignment.lower() == "end":
                day = calendar.monthrange(date_after.year, date_after.month)[1]
                return datetime.datetime(date_after.year, date_after.month, day).strftime("%Y-%m-%d")
            return date_after.strftime("%Y-%m-%d")
        elif delta.lower()=="day":
            date_after=datetime.datetime.strptime(date,"%Y-%m-%d")+relativedelta(days=n)
            return date_after.strftime("%Y-%m-%d")
    except:
        return "null"

def intck_(date1:str,date2:str,delta:str,method:str="discrete")->str:
    try:
        date1=datetime.datetime.strptime(date1,"%Y-%m-%d")
        date2=datetime.datetime.strptime(date2,"%Y-%m-%d")
        if delta.lower()=="day":
            return (date2-date1).days
        elif delta.lower()=="month" and method.lower()=="discrete":
            return (date2.year-date1.year)*12+date2.month-date1.month
    except:
        return "null"

class Skewness_:
    def __init__(self):
        self.sum1=0.0
        self.sum2=0.0
        self.sum3=0.0
        self.counter=0
    def step(self,value):
        self.sum1=self.sum1+value
        self.sum2=self.sum2+pow(value,2)
        self.sum3=self.sum3+pow(value,3)
        self.counter=self.counter+1
    def finalize(self):
        try:
            mean=self.sum1/self.counter
            variance=self.sum2-mean*self.sum1
            return (self.sum3-3*mean*variance-pow(mean,3))/pow(variance,1.5)
        except:
            return None

def prepare_helper_func(conn):
    conn.create_function("INTNX_",4,intnx_)
    conn.create_function("INTCK_",4,intck_)
    conn.create_aggregate("SKEWNESS_", 1, Skewness_)
    print("finished creating helper functions")
