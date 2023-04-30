from typing import List
from query_storage import query_storage

query_bank=query_storage()

def delete_table(cursor,tables:List[str]):
    query_bank=query_storage()
    query=query_bank.query_bank["utils"]["delete_table"]
    for table in tables:
        cursor.execute(query.format(table_name=table))
        cursor.fetchall()
        print("finished deleting table {}".format(table))
    print("finished deleting all tables")

def delete_column(cursor,table_columns:List[List[str]]):
    query_bank=query_storage()
    query=query_bank.query_bank["utils"]["delete_column"]
    for table,column in table_columns:
        cursor.execute(query.format(table_name=table,column_name=column))
        cursor.fetchall()
        print("finished deleting column {} in table {}".format(table,column))
    print("finished deleting all columns")

def rename_column(cursor,table_columns:List[List[str]]):
    query_bank=query_storage()
    query=query_bank.query_bank["utils"]["rename_column"]
    for table,column_name_old,column_name_new in table_columns:
        cursor.execute(query.format(table_name=table,column_name_old=column_name_old,column_name_new=column_name_new))
        cursor.fetchall()
        print("finished renaming column {} to {} in table {}".format(column_name_old,column_name_new,table))
    print("finished renaming all columns")

