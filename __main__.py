import argparse
import logging
import time
from logging.config import fileConfig

import loader
import merger
import writer

logger = logging.getLogger()

parser = argparse.ArgumentParser(description='Consolidate multiple parquet files into just one.')
parser.add_argument('--input-dir', metavar='-i', type=str, nargs=1,
                    help='input directories which contain a number of parquet files to be merged')
parser.add_argument('--product', metavar='-i', type=str, nargs=1,
                    help='name of the pair to extract (e.g. BTC-USD)')
parser.add_argument('--date', metavar='-d', type=str, nargs=1,
                    help='date to truncate the output to (only makes sense in the context of James Prince\'s project')
parser.add_argument('--output-dir', metavar='-o', type=str, nargs=1,
                    help='output directory for the parquet file (including .parquet extension)')


def get_times(section: int, num_sections: int) -> (str, str):
    section_length_in_hours = (24 / num_sections)
    start_hours = section * section_length_in_hours
    start_time = "%02i:00:00.00000.parquet" % start_hours

    end_hours = (section + 1) * section_length_in_hours
    end_time = "%02i:10:00.00000.parquet" % end_hours

    logger.debug("start_time: " + start_time)
    logger.debug("end_time: " + end_time)

    return start_time, end_time


def merge_data():
    t0 = time.time()

    num_sections = 24  # Enforce this to be a multiple of 24?
    for i in range(0, num_sections):
        section_t0 = time.time()

        start_time, end_time = get_times(i, num_sections)
        section_file_names = list(filter(lambda d: start_time < d < end_time, all_file_names))
        print(section_file_names)

        full_dirs = list(map(lambda x: data_root + x, section_file_names))
        logger.debug(full_dirs)

        # loaded_sets = loader.load_sets(full_dirs)
        merged_set = merger.merge_all_files(full_dirs, "BTC-USD")
        logger.debug(list(filter(lambda x: len(x) == 17, merged_set)))

        logger.info("Merge took: " + str(time.time() - section_t0))
        writing_t0 = time.time()

        ordered_df = writer.to_ordered_df(merged_set)

        ordered_df = ordered_df[ordered_df['time'].str.contains(date)].drop_duplicates()
        ordered_df.index = range(len(ordered_df.index))
        logger.debug(ordered_df)

        output_path = args.output_dir[0] + date + "-" + str(i) + ".parquet"
        logger.info("Writing to: " + output_path)
        writer.write_to_disk(ordered_df, output_path)

        logger.info("Writing took: " + str(time.time() - writing_t0))
        logger.info("Merge and write took: " + str(time.time() - section_t0))
    logger.info("Writing everything took: " + str(time.time() - t0))


if __name__ == "__main__":
    fileConfig('config/logging_config.ini')

    args = parser.parse_args()
    logger.debug(args)

    date = args.date[0]
    data_root = args.input_dir[0] + date + "/"

    all_file_names = loader.enum_all_files(data_root)
    print(all_file_names)
    # test_dirs = full_dirs[0:2]

    merge_data()


# # Use dfs throughout
# def task_2():
#     start_time = time.time()
#
#     loaded_dfs = map(loader.load_df, full_dirs[0:30])
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
