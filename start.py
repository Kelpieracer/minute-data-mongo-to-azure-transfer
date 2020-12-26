# Reads minute stock data from Yahoo and uploads it to Azure table storage
# (c) 2020 Kelpieracer

import os
from azure.cosmosdb.table.tableservice import TableService
from upsert_batch import insert_or_replace_week_of_minutedata

from dotenv import load_dotenv
load_dotenv()
account_key = os.environ.get("account_key")
account_name = os.environ.get("account_name")
table_name = os.environ.get("table_name")

table_client = TableService(
    account_name=account_name, account_key=account_key)

tickers = ['MES=F', 'M2K=F', '^OMXH25', 'MNQ=F', '^OMXHGI', '^GDAXI',
           'NOKIA.HE', 'OUT1V.HE', 'NESTE.HE', 'FORTUM.HE', '^OMXS30GI', 'TYRES.HE']

for ticker in tickers:
    insert_or_replace_week_of_minutedata(table_client, table_name, ticker)
