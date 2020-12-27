from datetime import datetime
from date_from_timestamp import date_from_timestamp
from azure.cosmosdb.table import TableBatch


def is_batch_full(count):
    return (count) % 100 == 0


def insert_or_replace_week_of_minutedata(table_client, table_name, ticker, mongo_cursor):
    print(ticker)
    batch = TableBatch()
    batch_row_count = 0
    for item in mongo_cursor:
        if(not item['isMarketOpen']):
            continue
        date = date_from_timestamp(item['timeStamp'])
        if(date >= datetime(2020, 12, 15)):
            continue
        my_entity = {
            'PartitionKey': ticker,
            'RowKey': date.strftime('%Y-%m-%d %H:%M:%S'),
            'Close': item['price'],
        }
        try:
            batch.insert_or_replace_entity(entity=my_entity)
        except:
            print('error')
        batch_row_count += 1
        if(is_batch_full(batch_row_count)):
            table_client.commit_batch(table_name=table_name, batch=batch)
            batch = TableBatch()
            print(str(batch_row_count) + ":" + my_entity['RowKey'], end='\r')

    if(not is_batch_full(batch_row_count)):
        table_client.commit_batch(table_name=table_name, batch=batch)
        print()
    print()
    print(ticker + ': ' + str(batch_row_count))
