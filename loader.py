import threading
from typing import List
import pandas as pd
import numpy as np
import time
# import pyarrow
from os import listdir
from os.path import isfile, join


data_root = "/Users/jamesprince/Google Drive/Imperial/4/Project/data/"
four_path = "4/23:57:07.290121.parquet"
five_path = "5/23:52:42.507663.parquet"
six_path = "6/23:56:36.712293.parquet"


def enum_all_files(path: str) -> List[str]:
    onlyfiles = [f for f in listdir(path) if isfile(join(path, f))]
    onlyfiles.sort()
    return onlyfiles


def load_df(path) -> pd.DataFrame:
    df: pd.DataFrame = pd.read_parquet(path)
    print(df.columns.values)
    print("df has "+str(len(df.columns.values))+" columns")
    return df.where((pd.notnull(df)), None)


def load_sets(paths: List[str]) -> List[set]:
    sets = []
    for path in paths:
        df = load_df(path)
        s = to_set(df)
        sets.append(s)
    return sets


# This code splits up a list into n slices of approximately the same length (useful for parallelisation)
# From: https://stackoverflow.com/questions/2130016/splitting-a-list-into-n-parts-of-approximately-equal-length
def chunkIt(seq, num):
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out



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
