import pandas as pd
import numpy as np
# import fastparquet

output_columns = ['client_oid', 'funds', 'maker_order_id', 'order_id',
                  'order_type', 'price', 'product_id', 'reason', 'remaining_size', 'sequence',
                  'side', 'size', 'taker_order_id', 'time', 'trade_id', 'type', 'new_size', 'old_size']


def to_ordered_df(s: set) -> pd.DataFrame:
    full_day = list(s)
    full_day.sort(key=lambda x: "" if not x[13] else x[13])
    full_day_df = pd.DataFrame(full_day, columns=output_columns)
    return full_day_df


def to_ordered_df_2(s: set) -> pd.DataFrame:
    full_day = list(s)
    full_day_df = pd.DataFrame(full_day, columns=output_columns)
    full_day_df.sort_values(by=['time'])
    return full_day_df


def write_to_disk(df: pd.DataFrame, filename) -> None:
    df.to_parquet(filename)

