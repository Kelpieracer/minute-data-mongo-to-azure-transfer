from datetime import datetime, timedelta


def date_from_timestamp(timestamp):
    date = datetime.fromtimestamp(timestamp/1000)
    milliseconds = date.microsecond/1000
    seconds = date.second
    return date - timedelta(milliseconds=milliseconds) - timedelta(seconds=seconds)


# print(date_from_timestamp(1585911844894.0/1000))
# print(date_from_timestamp(1545730073))
