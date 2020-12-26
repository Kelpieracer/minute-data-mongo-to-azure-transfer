import time
from finance import read_week_of_minutedata_from_yahoo
from azure.cosmosdb.table import TableBatch


def is_batch_full(count):
    return (count) % 100 == 0


def upsert_week_of_minutedata(table_client, table_name, ticker):
    time.sleep(2)
    print(ticker)
    MinuteItems = read_week_of_minutedata_from_yahoo(ticker)
    batch = TableBatch()
    batch_row_count = 0
    for index, minuteItem in MinuteItems.iterrows():
        rowKey = index.strftime('%Y-%m-%d %H:%M:%S')
        my_entity = {
            'PartitionKey': ticker,
            'RowKey': rowKey,
            'Open': minuteItem.Open.item(),
            'Close': minuteItem.Close.item(),
            'High': minuteItem.High.item(),
            'Low': minuteItem.Low.item(),
            'AdjClose': minuteItem['Adj Close'].item(),
            'Volume': minuteItem.Volume.item()
        }
        batch.insert_or_replace_entity(entity=my_entity)
        batch_row_count += 1
        if(is_batch_full(batch_row_count)):
            print(index, end='\r')
            table_client.commit_batch(table_name=table_name, batch=batch)
            batch = TableBatch()

    if(not is_batch_full(batch_row_count)):
        table_client.commit_batch(table_name=table_name, batch=batch)
    print()
    print(ticker + ': ' + str(batch_row_count))
