import logging
import time
from typing import Iterator, List

import numpy as np
import pandas as pd

import loader

input_columns = ['client_oid', 'funds', 'maker_order_id', 'new_size', 'old_size', 'order_id',
                 'order_type', 'price', 'product_id', 'reason', 'remaining_size', 'sequence',
                 'side', 'size', 'taker_order_id', 'time', 'trade_id', 'type']

logger = logging.getLogger()


def to_set(df: pd.DataFrame) -> set:
    # TODO: double check there aren't any other weird changes
    if len(df.columns.values) == 18:
        df = df[input_columns[0:3] + input_columns[5:] + input_columns[3:5]]
    elif len(df.columns.values) == 16:
        df['new_size'] = np.nan
        df['old_size'] = np.nan
    return set(map(tuple, df.values.tolist()))


def merge_sets(sets: List[set]) -> set:
    result = sets[0]
    for s in sets[1:]:
        result = result.union(s)
    return result


def merge_dfs(dfs: Iterator[pd.DataFrame]) -> pd.DataFrame:
    result = dfs.__next__()
    for df in dfs:
        result = result.append(df)
        result = result.drop_duplicates()
    result.sort_values(by='time')
    return result


def merge_all_files(paths: List[str], product: str) -> set:
    result = set([])
    for path in paths:
        logger.info("merging in: " + path)
        start_time = time.time()
        df = loader.load_df(path)
        # print("original: " + str(len(df['time'])))
        # print(df)
        df = df[df['product_id'] == product]
        # print("filtered: " + str(len(df['time'])))
        df_set = to_set(df)
        result = merge_sets([result, df_set])
        # print("result object size: " + str(sys.getsizeof(result) / 1000000)  + "MB")
        # print("took: " + str(time.time() - start_time) + " seconds")
        # print("result: " + str(len(result)))

        # for name, obj in (globals().items() + locals().items()):
        #     if name != 'asizeof':
        #         the_size = sys.getsizeof(obj) / 1000000
        #         # if the_size > 1:
        #         print(name + " " + str(the_size))
    return result
