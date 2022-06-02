import shutil
from typing import Iterator, Optional
from uuid import uuid4
import click
import numpy as np
import pandas as pd             # type: ignore
import pyarrow as pa            # type: ignore
import pyarrow.dataset as ds    # type: ignore
import pyarrow.parquet as pq    # type: ignore
from rich import print as rprint
from rich.pretty import pprint

from pathlib import Path


def _file_visitor(written_file) -> None:
    path: str = written_file.path
    metadata: pa._parquet.FileMetaData = written_file.metadata
    print(f'VISITOR: {path=}')
    print(f'VISITOR: {metadata=}')


def compact_dataset(
    input_path: Path,
    output_path: Path,
    input_filesystem: Optional[pa.fs.FileSystem] = None,
    output_filesystem: Optional[pa.fs.FileSystem] = None,
    verbose: bool = True,
) -> None:
    """
    Load a single parquet, transform it, append it to the output dataset
    """
    dataset = ds.dataset(input_path, filesystem=input_filesystem, partitioning='hive')
    scanner = dataset.scanner()  # https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html

    format = ds.ParquetFileFormat(
        # enable pre_buffer for high-latency filesystems
        # to read more than 1 col chunk per call
        pre_buffer=False,
        # use buffered stream to reduce memory usage
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
        file_visitor=_file_visitor if verbose else None,
    )


@click.command()
@click.option('--out-path', required=True, help='output dataset path')
@click.option('--in-path', required=True, help='input dataset path')
@click.option('--verbose', is_flag=True, show_default=False, default=False,
              help='Print info for each saved file')
def main(in_path, out_path, verbose) -> None:
    in_path = Path(in_path)
    out_path = Path(out_path)
    if not in_path.is_dir():
        raise FileNotFoundError(f'Path "{in_path}" does not exists')
    if out_path.is_dir():
        shutil.rmtree(out_path)

    compact_dataset(in_path, out_path, verbose=verbose)


if __name__ == '__main__':
    main()
    # main.callback(in_path='tiny_ingested_dataset', out_path='tiny_compacted_dataset')
