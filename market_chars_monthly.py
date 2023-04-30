
def market_chars_monthly(out,data,market_ret,local_currency):
    if local_currency==1:
        ret_var="ret_local"
    else:
        ret_var="ret"