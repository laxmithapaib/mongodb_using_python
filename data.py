from pymongo import MongoClient
import csv
import pandas as pd
import glob

# DB connectivity
client = MongoClient('localhost', 27017)
STOCKS = client.STOCKS
collection = STOCKS.collection

#reading file
path = r'C:\Users\paibl\Downloads'
all_files = glob.glob(path + "/*.csv")
li = []


for filename in all_files:
    df = pd.read_csv(filename, index_col=None, header=0)
    li.append(df)
    frame = pd.concat(li, axis=0, ignore_index=True)
    #fields = ['SYMBOL', 'SERIES']
    records_ = df.to_dict(orient='records')
    for i in records_:
        x= STOCKS.company_info.create_index({"symbol": i['SYMBOL'], "series": i[' SERIES'], "date1": i[' DATE1']})
        STOCKS.stock_info.createIndex({"prev_close": i[' PREV_CLOSE'], "open_price": i[' OPEN_PRICE'], "high_price": i[' HIGH_PRICE'], "low_price": i[' LOW_PRICE'], "last_price": i[' LAST_PRICE'], "close_price": i[' CLOSE_PRICE'], "avg_price": i[' AVG_PRICE']})
        STOCKS.trade_info.createIndex({"ttl_trd_qnty": i[' TTL_TRD_QNTY'], "turnover_lacs": i[' TURNOVER_LACS'], "no_of_trades": i[' NO_OF_TRADES'], "deliv_qty": i[' DELIV_QTY'], "deliv_per": i[' DELIV_PER']})
    #STOCKS.collection.insert_one("fields")
#STOCKS.docs.insert_many(records_)
''' 
# Function to parse csv to dictionary
df = pd.read_csv('10June.csv')
'''

# Final insert statement


