
import threading
from typing import Iterator, List
import pandas as pd
import numpy as np
import time
import loader
# import fastparquet
from os import listdir
from os.path import isfile, join


input_columns = ['client_oid', 'funds', 'maker_order_id', 'new_size', 'old_size', 'order_id',
                 'order_type', 'price', 'product_id', 'reason', 'remaining_size', 'sequence',
                 'side', 'size', 'taker_order_id', 'time', 'trade_id', 'type']


def to_set(df: pd.DataFrame) -> set:
    # TODO: double check there aren't any other weird changes
    if len(df.columns.values) == 18:
        df = df[input_columns[0:3] + input_columns[5:] + input_columns[3:5]]
    elif len(df.columns.values) == 16:
        df['new_size'] = np.nan
        df['old_size'] = np.nan
    return set(map(tuple, df.values.tolist()))


def merge_sets(sets: List[set]) -> set:
    result = set([])
    num_sets_merged = 0
    for s in sets:
        result = result.union(s)
        num_sets_merged += 1
        print("Merged in set number " + str(num_sets_merged))
    return result


def merge_dfs(dfs: Iterator[pd.DataFrame]) -> pd.DataFrame:
    result = dfs.__next__()
    for df in dfs:
        result = result.append(df)
    result.sort_values(by='time')
    return result


def merge_all_files(paths: List[str]) -> set:
    result = set([])
    for path in paths:
        print("merging in: " + path)
        start_time = time.time()
        df = loader.load_df(path)
        df_set = to_set(df)
        result = merge_sets([result, df_set])
        # print("took: " + str(time.time() - start_time) + " seconds")
        # print("result: " + str(len(result)))
    return result
