
def market_returns(out,data,freq,wins_comp,wins_data):
    if freq=="d":
        dt_col="date"
        max_date_lag=14
    elif freq=="m":
        dt_col="eom"
        max_date_lag=1
