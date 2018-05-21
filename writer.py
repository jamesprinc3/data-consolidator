import pandas as pd
import numpy as np

# import fastparquet

output_columns = ['time', 'client_oid', 'funds', 'maker_order_id', 'new_size', 'old_size', 'order_id',
                  'order_type', 'price', 'product_id', 'reason', 'remaining_size', 'sequence',
                  'side', 'size', 'taker_order_id', 'trade_id', 'type']


# TODO: move this function, it belongs in some other class!
def to_ordered_df(s: set) -> pd.DataFrame:
    lst = list(s)
    # lst = list(filter(lambda x: x[13], lst))
    for item in lst:
        if not str(item[0]).endswith("Z"):
            print(item[0])

    lst.sort(key=lambda x: "" if not x[0] else x[0])
    df = pd.DataFrame(lst, columns=output_columns)

    print("weird cols")
    for col in df.columns:
        weird = (df[[col]].applymap(type) != df[[col]].iloc[0].apply(type)).any(axis=1)
        if len(df[weird]) > 0:
            print(col)

    print(df['reason'].unique())

    return df


def to_ordered_df_2(s: set) -> pd.DataFrame:
    full_day = list(s)
    full_day_df = pd.DataFrame(full_day, columns=output_columns)
    full_day_df.sort_values(by=['time'])
    return full_day_df


def write_to_disk(df: pd.DataFrame, output_data_root, hour: int) -> None:
    # df['']
    filename = output_data_root + str("%02i" % hour)
    parquet_output_path = filename + ".parquet"
    csv_output_path = filename + ".csv"
    # logger.info("Writing to: " + parquet_output_path)

    df['size'] = pd.to_numeric(df['size'])
    df.to_parquet(parquet_output_path)
    df.to_csv(csv_output_path)
