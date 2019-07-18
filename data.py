import pandas as pd
import glob
from dbonnect import MDB

# DB connectivity using function

con = MDB()
db = con.get_collection()

# reading the csv files
path = r'C:\Users\paibl\Downloads\NSE'
all_files = glob.glob(path + "/*.csv")
li = []
for filename in all_files:
    df = pd.read_csv(filename, index_col=None, header=0)
    li.append(df)
    records = df.to_dict(orient='records')


# Final insert statement
    for i in records:
        db.company_info.insert_one({"symbol": i['SYMBOL'], "series": i[' SERIES'], "date1": i[' DATE1']})
        db.stock_info.insert_one({"prev_close": i[' PREV_CLOSE'], "open_price": i[' OPEN_PRICE'], "high_price": i[' HIGH_PRICE'], "low_price": i[' LOW_PRICE'], "last_price": i[' LAST_PRICE'], "close_price": i[' CLOSE_PRICE'], "avg_price": i[' AVG_PRICE']})
        db.trade_info.insert_one({"ttl_trd_qnty": i[' TTL_TRD_QNTY'], "turnover_lacs": i[' TURNOVER_LACS'], "no_of_trades": i[' NO_OF_TRADES'], "deliv_qty": i[' DELIV_QTY'], "deliv_per": i[' DELIV_PER']})

