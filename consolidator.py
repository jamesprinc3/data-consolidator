import pandas as pd
from os import listdir
from os.path import isfile, join
import datetime
from functools import reduce
import time
from memory_profiler import profile
# from profilehooks import profile

root_path = "~/parquet/"
outpu_path = "~/parquet-new/"


def mem_usage(df):
    for dtype in ['float', 'int', 'object']:
        selected_dtype = df.select_dtypes(include=[dtype])
        mean_usage_b = selected_dtype.memory_usage(deep=True).mean()
        mean_usage_mb = mean_usage_b / 1024 ** 2
        print("Average memory usage for {} columns: {:03.2f} MB".format(dtype, mean_usage_mb))

def generate_feed_path(exchange):
    today = datetime.datetime.utcnow().date()
    path = "parquet/" + exchange + "/orderbook/feed/"
    return path


def generate_trades_path(exchange):
    today = datetime.datetime.utcnow().date()
    time = datetime.datetime.utcnow().time()
    path = "parquet/" + exchange + "/orderbook/trades/" + str(today) + "/" + str(time) + ".parquet"
    return path

def get_parquet_files(path):
    return [f for f in listdir(path) if isfile(join(path, f)) and str.endswith(".parquet")]

def get_path_for_day(root_path, date: datetime.date):
    return root_path + date

# def load_day(date):
#
#
#
#     parquet_files = get_parquet_files()
#
#
#     for i in range()
#
#     pd.read_parquet(filename)


    #output graph/save to disk
# @profile
def consolidate(date):
    path = "/Users/jamesprince/" + generate_feed_path("gdax") + date + "/"
    print(path)
    chunks = [f for f in listdir(path) if isfile(join(path, f)) and f.endswith(".parquet")]
    chunks.sort()

    print(len(chunks))

    df = pd.read_parquet(path + chunks[0])

    for chunk in chunks[1:]:
        right = pd.read_parquet(path + chunk)
        df = df.append(right)
        # mem_usage(df)

    print(df)
    return df.drop_duplicates().sort_values(by=['time'])

def write_out(df: pd.DataFrame, date):
    path = "/Users/jamesprince/" + generate_feed_path("gdax") + date + ".parquet"
    df.to_parquet(path)


start = time.time()
date = "2018-01-30"
consol_df = consolidate(date)
write_out(consol_df, date)
end = time.time()
print(end - start)
