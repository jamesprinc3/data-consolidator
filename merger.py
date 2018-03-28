
import threading
from typing import Iterator, List
import pandas as pd
import numpy as np
import time
import loader
import fastparquet
from os import listdir
from os.path import isfile, join

columns = ['client_oid', 'funds', 'maker_order_id', 'order_id', 'order_type', 'price', 'product_id', 'reason', 'remaining_size', 'sequence', 'side', 'size', 'taker_order_id', 'time', 'trade_id', 'type']



#Can now use this as a reduction?
def merge_sets(sets: List[set]) -> set:
    result = set([])
    for s in sets:
        result = result.union(s)
    return result


def merge_dfs(dfs: Iterator[pd.DataFrame]) -> pd.DataFrame:
    result = dfs.__next__()
    for df in dfs:
        # print(df.columns.values)
        result = result.append(df)
        print(result)
    result.sort_values(by='time')
    return result

def merge_all_files(merge_root: str, paths: List[str]) -> set:
    result = set([])
    for path in paths:
        print("merging in: " + path)
        start_time = time.time()
        full_path = merge_root + path
        df = loader.load_df(full_path)
        df_set = loader.to_set(df)
        result = merge_sets([result, df_set])
        # print("took: " + str(time.time() - start_time) + " seconds")
        # print("result: " + str(len(result)))
    return result
