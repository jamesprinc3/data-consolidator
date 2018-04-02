
import threading
from typing import Iterator, List
import pandas as pd
import numpy as np
import time
import loader
# import fastparquet
from os import listdir
from os.path import isfile, join

columns = ['client_oid', 'funds', 'maker_order_id', 'order_id', 'order_type', 'price', 'product_id', 'reason', 'remaining_size', 'sequence', 'side', 'size', 'taker_order_id', 'time', 'trade_id', 'type']


# Can now use this as a reduction?
def merge_sets(sets: List[set]) -> set:
    result = set([])
    num_sets_merged = 1
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
        df_set = loader.to_set(df)
        result = merge_sets([result, df_set])
        # print("took: " + str(time.time() - start_time) + " seconds")
        # print("result: " + str(len(result)))
    return result
