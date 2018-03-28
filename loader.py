import threading
from typing import List
import pandas as pd
import numpy as np
import time
import fastparquet
from os import listdir
from os.path import isfile, join


data_root = "/Users/jamesprince/Google Drive/Imperial/4/Project/data/"
four_path = "4/23:57:07.290121.parquet"
five_path = "5/23:52:42.507663.parquet"
six_path = "6/23:56:36.712293.parquet"

merge_root = "/Users/jamesprince/Google Drive/Imperial/4/Project/data/merge/"


def enum_all_files(path: str) -> List[str]:
    onlyfiles = [f for f in listdir(path) if isfile(join(path, f))]
    onlyfiles.sort()
    return onlyfiles


file_list = enum_all_files(merge_root)

print(file_list)


def load_df(path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    # print(list(df.columns.values))
    return df.where((pd.notnull(df)), None)


def to_set(df: pd.DataFrame) -> set:
    return set(map(tuple, df.values.tolist()))


#Can now use this as a reduction?
def merge_sets(sets: List[set]) -> set:
    result = set([])
    for s in sets:
        result = result.union(s)
    return result


def merge_all_files(paths: List[str]) -> set:
    result = set([])
    for path in paths:
        print("merging in: " + path)
        start_time = time.time()
        full_path = merge_root + path
        df = load_df(full_path)
        df_set = to_set(df)
        result = merge_sets([result, df_set])
        print("took: " + str(time.time() - start_time) + " seconds")
        print("result: " + str(len(result)))


    return result

#From: https://stackoverflow.com/questions/2130016/splitting-a-list-into-n-parts-of-approximately-equal-length
def chunkIt(seq, num):
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out

start_time = time.time()

full_day_set = merge_all_files(file_list[0:2])

print("Merge took: " + str(time.time() - start_time))
start_time = time.time()

# make sorted df
full_day_list = list(full_day_set)
print(full_day_list[0][13])
full_day_list.sort(key=lambda x: "" if not x[13] else x[13])
full_day_df = pd.DataFrame(full_day_list, columns=['client_oid', 'funds', 'maker_order_id', 'order_id', 'order_type', 'price', 'product_id', 'reason', 'remaining_size', 'sequence', 'side', 'size', 'taker_order_id', 'time', 'trade_id', 'type'])

print(full_day_df)
print("DF conversion took: " + str(time.time() - start_time))
start_time = time.time()
#write to disk

full_day_df.to_parquet("2018-03-25.parquet")

print("Write to disk took: " + str(time.time() - start_time))



# threads = []
# num_threads = 4
# file_lists = chunkIt(file_list, num_threads)
# for i in range(num_threads):
#     t = threading.Thread(target=merge_all_files, args=(file_lists[i],))
#     threads.append(t)
#     t.start()



#
#
# four_df: pd.DataFrame = load_df(data_root + four_path)
# five_df: pd.DataFrame = load_df(data_root + five_path)
# six_df: pd.DataFrame = load_df(data_root + six_path)
#
# print("Load took: " + str(time.time() - start_time))
# start_time = time.time()
# # dfs = [four_df, five_df, six_df]
# dfs = [four_df, five_df, six_df]
#
# merge_dfs(dfs)
#
#
