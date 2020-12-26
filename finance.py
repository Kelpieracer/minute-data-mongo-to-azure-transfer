import yfinance as yf


def ReadStocks(ticker, endDate):
    data = yf.download(ticker, end=endDate, period='5d',
                       interval='1m', group_by="ticker")
    return data
