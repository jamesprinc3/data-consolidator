from typing import List
import pandas as pd
import numpy as np
import time
import fastparquet


data_root = "/Users/jamesprince/Google Drive/Imperial/4/Project/data/"
four_path = "4/23:57:07.290121.parquet"
five_path = "5/23:52:42.507663.parquet"
six_path = "6/23:56:36.712293.parquet"


def load_df(path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    return df.where((pd.notnull(df)), None)


def merge_dfs(dfs: List[pd.DataFrame]) -> None:
    start_time = time.time()

    master = set([])
    sets: List[set] = []
    for df in dfs:
        sets.append(set(map(tuple, df.values.tolist())))

    for s in sets:
        print("set size: " + str(len(s)))
    #TODO: obviously make this more generic and clean it up
    union = sets[0] | sets[1] | sets[2]
    print("first union: " )
    # union = union.union(sets[2])
    # print("second union: " + str(time.time() - start_time))

    print("set size: " + str(len(union)))
    return time.time() - start_time

start_time = time.time()

four_df: pd.DataFrame = load_df(data_root + four_path)
five_df: pd.DataFrame = load_df(data_root + five_path)
six_df: pd.DataFrame = load_df(data_root + six_path)

print("Load took: " + str(time.time() - start_time))

# dfs = [four_df, five_df, six_df]
dfs = [four_df, five_df, six_df]

print("Merge took: " + str(merge_dfs(dfs)))
