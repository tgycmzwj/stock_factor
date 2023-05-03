from typing import List
import pandas as pd
from query_storage import query_storage


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
            print("finished deleting table {}".format(table))
        print("finished deleting all tables")

    def delete_column(self,table_columns:List[List[str]]):
        query=self.query_bank["delete_column"]
        for table,column in table_columns:
            self.cursor.execute(query.format(table_name=table,column_name=column))
            self.cursor.fetchall()
            self.conn.commit()
            print("finished deleting column {} in table {}".format(table,column))
        print("finished deleting all columns")

    def rename_table(self,tables:List[List[str]]):
        query=self.query_bank["rename_table"]
        for table_name_old,table_name_new in tables:
            self.cursor.execute(query.format(table_name_old=table_name_old,table_name_new=table_name_new))
            self.cursor.fetchall()
            self.conn.commit()
            print("finished renaming table {} to {}".format(table_name_old,table_name_new))
        print("finished renaming all tables")

    def rename_column(self,table_columns:List[List[str]]):
        query=self.query_bank["rename_column"]
        for table,column_name_old,column_name_new in table_columns:
            self.cursor.execute(query.format(table_name=table,column_name_old=column_name_old,column_name_new=column_name_new))
            self.cursor.fetchall()
            self.conn.commit()
            print("finished renaming column {} to {} in table {}".format(column_name_old,column_name_new,table))
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
        print("finished sorting table {}".format(table))

    def sort_and_remove_duplicates(self,table_in,table_out,sortvar,idvar=None):
        query=self.query_bank["sort_and_remove_duplicates"]
        self.cursor.execute(query.format(table_in=table_in,sortvar=''.join(sortvar),idvar=''.join(idvar) if idvar is not None else ''.join(sortvar)))
        self.cursor.fetchall()
        self.delete_column([table_in,"row_number"])
        if table_out==table_in:
            self.delete_table([table_in])
            self.rename_table([[table_in+"_sorted",table_in]])
        else:
            self.rename_table([[table_in+"_sorted",table_out]])

        print("finished sorting and removing duplicates for table {}".format(table_in))

if __name__=="__main__":
    obj=utils()
    print("finished")