"""
Script to perform compaction of a parquet dataset.

For usage see:

    $ python compaction.py --help

"""
from typing import Optional
from pathlib import Path
import shutil
from uuid import uuid4
import click

import pyarrow as pa             # type: ignore
import pyarrow.dataset as ds     # type: ignore
import pyarrow.filesystem as fs  # type: ignore


# See dataset.write_dataset docs:
# https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html
DEFAULT_MAX_ROWS_PER_FILE = 16 * 1024 * 1024   # pyarrow default: 0
DEFAULT_MIN_ROWS_PER_GROUP = 128 * 1024        # pyarrow default: 0
DEFAULT_MAX_ROWS_PER_GROUP = 1024 * 1024       # pyarrow default: 1024 * 1024


def _file_visitor(written_file) -> None:
    """Callback called for each parquet file saved by `ds.write_dataset()`
    """
    path: str = written_file.path
    metadata: pa._parquet.FileMetaData = written_file.metadata
    print(f'VISITOR: {path=}')
    print(f'VISITOR: {metadata=}')


def compact_dataset(
    input_path: Path,
    output_path: Path,
    input_filesystem: Optional[fs.FileSystem] = None,
    output_filesystem: Optional[fs.FileSystem] = None,
    max_rows_per_file: int = DEFAULT_MAX_ROWS_PER_FILE,
    min_rows_per_group: int = DEFAULT_MIN_ROWS_PER_GROUP,
    max_rows_per_group: int = DEFAULT_MAX_ROWS_PER_GROUP,
    verbose: bool = True,
) -> None:
    """
    Load a single parquet, transform it, append it to the output dataset
    """
    dataset = ds.dataset(input_path, filesystem=input_filesystem,
                         partitioning='hive')

    # https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html
    scanner = dataset.scanner()

    format = ds.ParquetFileFormat(
        # enable pre_buffer for high-latency filesystems (cloud object stores)
        # to read more than 1 col chunk per call
        pre_buffer=False,

        # Use buffered stream to reduce memory usage. Keep it False,
        # unless row groups are too large to fit in the available RAM
        use_buffered_stream=False, buffer_size=16*1024,
    )
    write_options = format.make_write_options(
        use_dictionary=True, compression='snappy', version='2.6')
    ds.write_dataset(
        scanner,

        base_dir=output_path,
        filesystem=output_filesystem,

        format=format,
        file_options=write_options,

        partitioning=['year', 'month'],
        partitioning_flavor='hive',

        existing_data_behavior='delete_matching',
        basename_template=f'{uuid4()}-{{i}}.parquet',

        max_rows_per_file=max_rows_per_file,
        min_rows_per_group=min_rows_per_group,
        max_rows_per_group=max_rows_per_group,

        file_visitor=_file_visitor if verbose else None,
    )


@click.command()
@click.option('--in-path', required=True, help='input dataset path')
@click.option('--out-path', required=True, help='output dataset path')
@click.option('--max-rows-per-file', default=16 * 1024 * 1024,
              help='Split parquet files with more rows than this value')
@click.option('--min-rows-per-group', default=DEFAULT_MIN_ROWS_PER_GROUP,
              help='Batch rows up to this value before saving a row group')
@click.option('--max-rows-per-group', default=DEFAULT_MAX_ROWS_PER_GROUP,
              help='Split row groups larger than this value')
@click.option('--verbose', is_flag=True, show_default=False, default=False,
              help='Print info for each saved file')
def main(in_path, out_path, verbose, max_rows_per_file,
         min_rows_per_group, max_rows_per_group) -> None:
    """
    Perform compaction of a parquet dataset
    """
    in_path = Path(in_path)
    out_path = Path(out_path)
    if not in_path.is_dir():
        raise FileNotFoundError(f'Path "{in_path}" does not exists')
    if out_path.is_dir():
        shutil.rmtree(out_path)

    compact_dataset(in_path, out_path, max_rows_per_file=max_rows_per_file,
                    min_rows_per_group=min_rows_per_group,
                    max_rows_per_group=max_rows_per_group,
                    verbose=verbose)


if __name__ == '__main__':
    main()

    # Test call with hardcoded args, to be used with debugger
    # main.callback(in_path='tiny_ingested_dataset', out_path='tiny_compacted_dataset')
