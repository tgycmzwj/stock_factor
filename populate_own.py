import pandas as pd


def populate_own(conn,inset,outset,datevar,idvar,datename,forward_max,period):
    """
    :param inset: str. The name of the input dataset
    :param outset: str. The name of the output dataset
    :param datevar: str. The name of the date variable
    :param idvar: str. the name of the id variable
    :param datename: str. The name of the new date variable to be created
    :param forward_max: str. The regular periodicity or forward population intervals
    :param period: str. day or month
    :return:
    """
    #read dataset
    df=pd.read_sql_query("SELECT * FROM {inset}".format(inset=inset))
    #drop duplicates on id var and datevar
    df=df.drop_duplicates(subset=[idvar,datevar],keep="last")
    df=df.sort_values(by=[idvar,datevar],ascending=[False,False])
    df[datename]=pd.to_datetime(df[datevar],format="%y%m%d")

    #populate dates
    #FORWARD_MAX is the regular periodicity or the forward population intervals
    nid=len(idvar)
    id2=idvar[-1]
    df_out=pd.DataFrame(columns=[idvar,datevar,datename])
    df_out[datename]=pd.to_datetime(df_out[datename],format="%y%m%d")

    for group_name, group_data in df.groupby(idvar):
        group_data[datename] = group_data[datevar]
        df_out = pd.concat([df_out, group_data])
        df_out = df_out.sort_values(by=[idvar, datename], ascending=[False, True])
        following = df_out.groupby(idvar)[datename].shift(1)
        df_out['following'] = following

        # Check if the first record of each group, then set following to missing
        df_out.loc[df_out.groupby(idvar)[datevar].head(1).index, 'following'] = pd.NaT

        forward_max = pd.to_datetime(forward_max, format='%y%m%d')
        df_out['n'] = (df_out[datename].astype(int) - df_out['following'].astype(int)).div(
            10 ** 9 * 60 * 60 * 24).astype(int)
        df_out['n'] = df_out['n'].apply(lambda x: min(x, (forward_max - df_out[datename].iloc[-1]).days))
        df_out = df_out.reset_index(drop=True)

        for idx, row in df_out.iterrows():
            n = row['n']
            for i in range(1, n):
                new_date = row[datename] + pd.DateOffset(days=i)
                new_row = [row[idvar], row[datevar], new_date]
                df_out.loc[len(df_out)] = new_row

        df_out = df_out.drop(columns=['following', 'n'])

    df_out = df_out.sort_values(by=[idvar, datevar, datename]).drop_duplicates(subset=[idvar, datevar, datename],
                                                                               keep='last')
    df_out.to_sql(outset, conn, if_exists='replace', index=False)
    # House Cleaning
    conn.execute('DROP TABLE __temp')
    print(f'### DONE. Dataset {outset} with {period} Frequency Generated')





