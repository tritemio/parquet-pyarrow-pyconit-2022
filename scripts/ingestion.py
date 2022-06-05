"""
Script to perform ingestion of a parquet dataset.

For details see:

    $ python ingestion.py --help

"""
from typing import Iterator, Optional
import shutil
from pathlib import Path
from uuid import uuid4
import click
from rich import print as rprint
from rich.pretty import pprint

import pyarrow as pa             # type: ignore
import pyarrow.dataset as ds     # type: ignore
import pyarrow.parquet as pq     # type: ignore
import pyarrow.filesystem as fs  # type: ignore


def _file_visitor(written_file) -> None:
    """Callback called for each parquet file saved by `ds.write_dataset()`
    """
    path: str = written_file.path
    metadata: pa._parquet.FileMetaData = written_file.metadata
    print(f'VISITOR: {path=}')
    print(f'VISITOR: {metadata=}')


def get_batch_queue(path: Path) -> Iterator[Path]:
    for parquet_file_path in path.glob('**/*.parquet'):
        yield parquet_file_path


def showcase_inspect_single_parquet_file(path: Path) -> None:
    """Demonstration of inspecting a parquet file.
    """
    bar = f'[green]{"=" * 20}[/green]'
    rprint(f'\n{bar}\nDS File: {path}\n{bar}\n')

    # This is only a pointer to the dataset
    dataset = ds.dataset(path)
    rprint(f'* Dataset Schema:\n{dataset.schema}\n')

    # By default, a fragment corresponds to a file
    # (but, for custom built datasets, a fragment could map to file subset)
    fragment = list(dataset.get_fragments())[0]
    file = dataset.files[0]
    assert fragment.path == file
    rprint(f'\n>>> {fragment.metadata=}')
    # rprint(f'* Fragment Metadata:\n{fragment.metadata}')

    # This dataset has only 1 row group per file
    row_group = fragment.row_groups[0]

    rprint(f'\n>>> {row_group.metadata=}')
    rprint('\n* Row group statistics:')
    pprint(row_group.statistics)

    df = dataset.to_table()
    pass


def showcase_load_single_file_with_ParquetFile(path: Path) -> None:
    """Demonstration of using `pyarrow.parquet.ParquetFile`
    """
    line = '=' * 20
    rprint(f'\n[green]{line}[/green]\nPQ File:    {path}\n[green]{line}[/green]\n')

    # A pointer to a single parquet file
    parquet_file = pq.ParquetFile(path)
    rprint(f'* Parquet File Schema:\n{parquet_file.schema}\n')
    rprint(f'* Parquet File Metadata:\n{parquet_file.metadata}\n')

    table = parquet_file.read()
    pass


def process_table(table: pa.Table) -> pa.Table:
    """Process a table adding year, month and day columns.
    """
    df = table.to_pandas()
    dt_series = df['datetime']

    datetime_series = {
        'year': dt_series.dt.year,
        'month': dt_series.dt.month,
        'day': dt_series.dt.day
    }
    for name, series in datetime_series.items():
        array = pa.Array.from_pandas(series)
        field = pa.field(name, array.type)
        table = table.append_column(field, array)
    return table


def process_single_file(
    input_path: Path,
    output_path: Path,
    input_filesystem: Optional[fs.FileSystem] = None,
    output_filesystem: Optional[fs.FileSystem] = None,
    verbose: bool = True,
) -> None:
    """
    Load a single parquet, transform it, append it to the output dataset
    """
    dataset = ds.dataset(input_path, filesystem=input_filesystem)
    table = dataset.to_table()

    out_table = process_table(table)

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
        out_table,
        base_dir=output_path,
        filesystem=output_filesystem,
        format=format,
        file_options=write_options,
        partitioning=['year', 'month'],
        partitioning_flavor='hive',
        existing_data_behavior='overwrite_or_ignore',
        basename_template=f'{uuid4()}-{{i}}.parquet',
        file_visitor=_file_visitor if verbose else None,
    )


@click.command()
@click.option('--out-path', required=True, help='output dataset path')
@click.option('--in-path', required=True, help='input dataset path')
@click.option('--verbose', is_flag=True, show_default=False, default=False,
              help='Print info for each saved file')
def main(in_path, out_path, verbose) -> None:
    """
    Perform ingestion of a parquet dataset.

    Takes a flat folder of parquet files and resaves it as a new dataset
    partitioned by year, month.
    """
    in_path = Path(in_path)
    out_path = Path(out_path)
    if not in_path.is_dir():
        raise FileNotFoundError(f'Path "{in_path}" does not exists')
    if out_path.is_dir():
        shutil.rmtree(out_path)

    batch_queue = get_batch_queue(in_path)
    for parquet_file in batch_queue:
        process_single_file(parquet_file, out_path, verbose=verbose)
        # break


if __name__ == '__main__':
    main()

    # Test call with hardcoded args, to be used with debugger
    # main.callback(out_path='ingested_dataset')
