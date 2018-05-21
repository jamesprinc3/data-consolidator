import logging
from typing import Iterator, List

import pandas as pd

import loader

cols = ['time', 'client_oid', 'funds', 'maker_order_id', 'new_size', 'old_size', 'order_id',
                 'order_type', 'price', 'product_id', 'reason', 'remaining_size', 'sequence',
                 'side', 'size', 'taker_order_id', 'trade_id', 'type']

logger = logging.getLogger()


def to_set(df: pd.DataFrame) -> set:
    # TODO: double check there isn't any other weirdness
    if 'time' in cols:
        cols.remove('time')
    df['time'] = df['time'].dropna()

    optional_cols = ['funds', 'new_size', 'old_size', 'maker_order_id', 'taker_order_id', 'trade_id']

    for col in optional_cols:
        if col not in df:
            df[col] = pd.np.nan

    df = df[['time'] + cols]
    print("columns: " + str(list(df.columns.values)))
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
        df = loader.load_df(path)
        df = df[df['product_id'] == product]
        logger.debug(df.columns)
        df_set = to_set(df)
        result = merge_sets([result, df_set])
    return result
