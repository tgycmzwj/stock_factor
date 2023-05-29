from typing import List
import pandas as pd
from query_storage import query_storage
import time

class utils(object):
    def __init__(self,conn,cursor):
        query_bank_obj=query_storage()
        self.query_bank=query_bank_obj.query_bank["utils"]
        self.cursor=cursor
        self.conn=conn

    def delete_table(self,tables:List[str]):
        query=self.query_bank["delete_table"]
        for table in tables:
            self.cursor.execute(query.format(table_name=table))
            self.cursor.fetchall()
            print("finished deleting table {} at time {}".format(table,time.asctime()))
        print("finished deleting all tables")

    def delete_column(self,table_columns:List[List[str]]):
        query=self.query_bank["delete_column"]
        for table,column in table_columns:
            self.cursor.execute(query.format(table_name=table,column_name=column))
            self.cursor.fetchall()
            self.conn.commit()
            print("finished deleting column {} in table {} at time {}".format(column,table,time.asctime()))
        print("finished deleting all columns")


    def keep_column(self,table:str,columns:List[str]):
        query=self.query_bank["keep_columns"]
        self.cursor.execute(query.format(table_name=table,column_name=','.join(columns)))
        self.cursor.fetchall()
        self.delete_table([table])
        self.rename_table([[table+"_new",table]])


    def rename_table(self,tables:List[List[str]]):
        query=self.query_bank["rename_table"]
        for table_name_old,table_name_new in tables:
            self.cursor.execute(query.format(table_name_old=table_name_old,table_name_new=table_name_new))
            self.cursor.fetchall()
            self.conn.commit()
            print("finished renaming table {} to {} at time {}".format(table_name_old,table_name_new,time.asctime()))
        print("finished renaming all tables")

    def rename_column(self,table_columns:List[List[str]]):
        query=self.query_bank["rename_column"]
        for table,column_name_old,column_name_new in table_columns:
            self.cursor.execute(query.format(table_name=table,column_name_old=column_name_old,column_name_new=column_name_new))
            self.cursor.fetchall()
            self.conn.commit()
            print("finished renaming column {} to {} in table {} at time {}".format(column_name_old,column_name_new,table,time.asctime()))
        print("finished renaming all columns")

    def return_column(self,columns:List[str],table:str):
        query=self.query_bank["return_column"]
        return pd.read_sql(query.format(column_name=','.join(columns),table_name=table),self.conn)

    def sort_table(self,table,sortvar):
        query=self.query_bank["sort_table"]
        self.cursor.execute(query.format(table_name=table,sortvar=','.join(sortvar)))
        self.cursor.fetchall()
        self.delete_table([table])
        self.rename_table([[table+"_sorted",table]])
        print("finished sorting table {} at time {}".format(table,time.asctime()))

    def sort_and_remove_duplicates(self,table_in,table_out,sortvar,idvar=None):
        query=self.query_bank["sort_and_remove_duplicates"]
        self.cursor.execute(query.format(table_in=table_in,sortvar=sortvar,idvar=idvar if idvar is not None else sortvar))
        self.cursor.fetchall()
        self.delete_column([[table_in+"_sorted","row_number"]])
        if table_out==table_in:
            self.delete_table([table_in])
            self.rename_table([[table_in+"_sorted",table_in]])
        else:
            self.rename_table([[table_in+"_sorted",table_out]])
        print("finished sorting and removing duplicates for table {} at time {}".format(table_in,time.asctime()))

    def duplicate_records(self,table_in,table_out,num):
        query=self.query_bank["duplicate_on"]
        self.cursor.execute(query.format(table_in=table_in,table_out=table_out,num=num))
        self.cursor.fetchall()
        print("finishing duplicating records for table {} based on column {} at time {}".format(table_in,num,time.asctime()))

    def create_index(self,table_name,index_name,column_name):
        query=self.query_bank["create_index"]
        self.cursor.execute(query.format(table_name=table_name,index_name=index_name,column_name=column_name))
        self.cursor.fetchall()
        print("finishing creating index {} for table {} based on column {} at time {}".format(index_name,table_name,column_name,time.asctime()))

    def drop_index(self,index_name):
        query=self.query_bank["drop_index"]
        self.cursor.execute(query.format(index_name=index_name))
        self.cursor.fetchall()
        print("finishing dropping index {} at time {}".format(index_name,time.asctime()))

    def change_column_type(self,table_name,column_name,column_type):
        query=self.query_bank["change_column_type"]
        self.cursor.execute(query.format(table_name=table_name,column_name=column_name,column_type=column_type))
        self.cursor.fetchall()
        self.delete_table([table_name])
        self.rename_table([[table_name+"_new",table_name]])
        self.delete_column([[table_name,column_name]])
        self.rename_column([[table_name,column_name+"_new",column_name]])

    def list_index(self):
        query=self.query_bank["list_index"]
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def list_table(self):
        query=self.query_bank["list_table"]
        self.cursor.execute(query)
        results=self.cursor.fetchall()
        return [item[0] for item in results]

    def list_column(self,table_name):
        query=self.query_bank["list_column"]
        self.cursor.execute(query.format(table_name=table_name))
        results=self.cursor.fetchall()
        return [item[1] for item in results]

class executor(object):
    def __init__(self,conn,cursor):
        self.cursor=cursor
        self.conn=conn
    def execute_and_commit(self,query):
        self.cursor.execute(query)
        self.conn.commit()
        print("finished query {} at time {}".format(query[:100].replace("/n"," "),time.asctime()))
        return self.cursor.fetchall()

if __name__=="__main__":
    obj=utils()
    print("finished")