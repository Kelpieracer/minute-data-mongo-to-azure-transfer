# Reads minute stock data from MongoDB and uploads it to Azure table storage
# (c) 2020 Kelpieracer

from dotenv import load_dotenv
from getmongo import get_cursor_to_ticker_minutedata
from upsert_batch import insert_or_replace_week_of_minutedata
from azure.cosmosdb.table.tableservice import TableService
import os
tickers = ['MES=F', 'M2K=F', '^OMXH25', 'MNQ=F', '^OMXHGI', '^GDAXI',
           'NOKIA.HE', 'OUT1V.HE', 'NESTE.HE', 'FORTUM.HE', '^OMXS30GI', 'TYRES.HE']


load_dotenv()
account_key = os.environ.get("account_key")
account_name = os.environ.get("account_name")
table_name = os.environ.get("table_name")

table_client = TableService(
    account_name=account_name, account_key=account_key)

for ticker in tickers:
    mongo_cursor = get_cursor_to_ticker_minutedata(ticker)
    insert_or_replace_week_of_minutedata(
        table_client=table_client, table_name=table_name, ticker=ticker, mongo_cursor=mongo_cursor)
