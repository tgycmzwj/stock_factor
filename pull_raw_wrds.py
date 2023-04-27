import time
def pull_raw_wrds(datasets,wrds_db,local_conn,local_cursor,update=False):
    for data in datasets:
        try:
            localdata="_".join(data.split("."))
            print("starts to pull dataset "+localdata+" "+time.asctime())
            # check if the data is already stored locally
            query_table_name = "select name from sqlite_master where type='table'"
            local_cursor.execute(query_table_name)
            existed_tables = local_cursor.fetchall()
            existed_tables=[item[0] for item in existed_tables]
            lib, table = data.split('.')
            if localdata in existed_tables and update==False:
                print("skip dataset "+localdata)
                continue
            # fetch data from wrds
            wrds_table = wrds_db.get_table(lib, table)
            print("Successfully retrieve data "+localdata)
            # drop duplicates
            wrds_table.drop_duplicates(inplace=True)
            # store data locally
            wrds_table.to_sql(lib+"_"+table,local_conn,if_exists="replace",index=False)
            print("Finish pulling dataset "+localdata+" "+time.asctime())
        except:
            print("Fail to pull dataset "+localdata)


# compustat_g_secd=pd.read_csv("/kellogg/proj/wzi1638/stock_factor/compustat_g_secd.csv")
# compustat_g_secd.to_sql("comp_g_secd", conn, if_exists="replace", index=False)
