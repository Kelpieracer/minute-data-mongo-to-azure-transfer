import os
from pymongo.mongo_client import MongoClient
from dotenv import load_dotenv

load_dotenv()
mongodb_uri = os.environ.get("mongodb")
myclient = MongoClient(mongodb_uri)

mydb = myclient["QuickStocks"]
mycol = mydb["Indexes"]


def get_cursor_to_ticker_minutedata(ticker):
    myquery = {"ticker": ticker}
    mydoc = mycol.find(myquery).batch_size(10000)
    return mydoc
