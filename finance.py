import yfinance as yf


def read_week_of_minutedata_from_yahoo(ticker):
    data = yf.download(ticker, interval='1m', period='7d', group_by="ticker")
    return data
