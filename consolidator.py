import numpy as np
import pandas as pd
import time
import loader
import merger
import writer

import argparse

parser = argparse.ArgumentParser(description='Consolidate multiple parquet files into just one.')
parser.add_argument('input_dir', metavar='-i', type=str, nargs=1,
                    help='input directories which contain a number of parquet files to be merged')
parser.add_argument('date', metavar='-d', type=str, nargs=1,
                    help='date to truncate the output to (only makes sense in the context of James Prince\'s project')
parser.add_argument('output_dir', metavar='-o', type=str, nargs=1,
                    help='output directory for the parquet file (including .parquet extension)')

args = parser.parse_args()
print(args.input_dir)
print(args.date)
print(args.output_dir)

data_root = args.input_dir[0]
date = args.date[0]
output_path = args.output_dir[0] + date + ".parquet"

file_names = loader.enum_all_files(data_root)
print(file_names)
full_dirs = list(map(lambda x: data_root + x, file_names))
# test_dirs = full_dirs[0:2]


def merge_data():
    start_time = time.time()

    # loaded_sets = loader.load_sets(full_dirs)
    merged_set = merger.merge_all_files(full_dirs[20:23])
    print(list(filter(lambda x: len(x) == 17, merged_set)))

    print("Merge took: " + str(time.time() - start_time))
    start_time = time.time()

    ordered_df = writer.to_ordered_df(merged_set)

    ordered_df = ordered_df[ordered_df['time'].str.contains(date)]
    ordered_df.index = range(len(ordered_df.index))
    print(ordered_df)
    writer.write_to_disk(ordered_df, output_path)

    print("Writing took: " + str(time.time() - start_time))


# # Use dfs throughout
# def task_2():
#     start_time = time.time()
#
#     loaded_dfs = map(loader.load_df, test_dirs)
#     ordered_df = merger.merge_dfs(loaded_dfs)
#
#     print(ordered_df)
#
#     print("Merge took: " + str(time.time() - start_time))
#     start_time = time.time()
#
#     writer.write_to_disk(ordered_df, "2018-03-25-2")
#
#     print("Writing took: " + str(time.time() - start_time))
#
#
# # Sort values inside the DataFrame
# def task_3():
#     start_time = time.time()
#
#     loaded_sets = loader.load_sets(test_dirs)
#     merged_set = merger.merge_sets(loaded_sets)
#
#     print("Merge took: " + str(time.time() - start_time))
#     start_time = time.time()
#
#     ordered_df = writer.to_ordered_df_2(merged_set)
#     writer.write_to_disk(ordered_df, "2018-03-25-3")
#
#     print("Writing took: " + str(time.time() - start_time))


if __name__ == "__main__":
    merge_data()

