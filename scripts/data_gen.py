"""
Script to generate a flat parquet dataset with random data.

For details see:

    $ python data_gen.py --help

"""

from pathlib import Path
import shutil
from typing import Any, Iterator
from uuid import uuid4
import numpy as np
import pandas as pd             # type: ignore
import pyarrow as pa            # type: ignore
import pyarrow.dataset as ds    # type: ignore
import pyarrow.parquet as pq    # type: ignore
import click


def get_batch(
    n_rows: int,
    datetime_start: pd.Timestamp, datetime_stop: pd.Timestamp,
    rng: np.random.Generator | int = 1,
    batch_index: int = 1,
) -> pa.RecordBatch:
    """
    Generate random data and return it as a pa.RecordBatch

    Args:
        n_rows: number of rows in the batch
        datetime_start: minimum datetime value (inclusive)
        datetime_stop: maximum datetime value (exclusive)

    Return:
        A pa.RecordBatch or randomly-generated data with datetime column
    """
    if isinstance(rng, int):
        rng = np.random.default_rng(rng)

    categories_list = [
        ['foo', 'bar', 'baz'],
        ['red', 'green', 'blue'],
        ['fizz', 'buzz']
    ]
    n_numeric_cols = 10

    categorical_data = {
        f"cat_col_{i:02}": pa.DictionaryArray.from_arrays(
            pa.array(rng.integers(low=0, high=len(categories), size=n_rows)),
            pa.array(categories))
        for i, categories in enumerate(categories_list)
    }
    numerical_data = {
        f"num_col_{i:02}": pa.array(rng.normal(size=n_rows))
        for i in range(n_numeric_cols)
    }

    datetime_data = {
        "datetime": pd.date_range(
            datetime_start, datetime_stop, periods=n_rows + 1)[:-1]
        }
    # dt_series = pd.Series(datetime_data["datetime"])
    # datetime_data['year'] = dt_series.dt.year
    # datetime_data['month'] = dt_series.dt.month
    # datetime_data['day'] = dt_series.dt.day

    batch_col = {'batch': np.repeat(batch_index, n_rows)}
    batch = pa.RecordBatch.from_pydict(
        datetime_data | categorical_data | numerical_data | batch_col)
    return batch


def batch_generator(n_batches: int, batch_size: int = 1000,
                    batch_duration: pd.Timedelta = pd.Timedelta(1, unit='day')
                    ) -> Iterator[pa.RecordBatch]:
    rng = np.random.default_rng(1)

    stop = pd.Timestamp('2020-01-01')
    for i in range(n_batches):
        start = stop
        stop += batch_duration
        batch = get_batch(batch_size, start, stop, rng=rng, batch_index=i)
        yield batch


def get_batch_schema() -> pa.Schema:
    # Get one batch only to extract the schema
    rng = np.random.default_rng(1)
    start = pd.Timestamp('2020-03-30')
    stop = start + pd.Timedelta(1, unit='day')
    batch = get_batch(1, start, stop, rng=rng)
    return batch.schema


def _file_visitor(written_file) -> None:
    path: str = written_file.path
    metadata: pa._parquet.FileMetaData = written_file.metadata
    print(f'VISITOR: {path=}')
    print(f'VISITOR: {metadata=}')


def create_dataset(path: str,
                   batch_iterator: Iterator[pa.RecordBatch],
                   compact: bool = True,
                   verbose: bool = False,
                   ) -> None:
    """
    Create a partitioned parquet dataset.
    """
    if Path(path).is_dir():
        shutil.rmtree(path)

    format = ds.ParquetFileFormat()
    write_options = format.make_write_options(
        use_dictionary=True, compression='snappy', version='2.6')

    write_kwargs: dict[str, Any] = dict(
        base_dir=path,
        format=format,
        file_options=write_options,
        # partitioning=['year', 'month'],
        partitioning_flavor='hive',
        # max_rows_per_group=batch_size,
        # max_rows_per_file=batch_size,
    )
    if verbose:
        write_kwargs['file_visitor'] = _file_visitor

    if compact:
        # Write a dataset with one file per partition
        # and one row group per batch
        schema = get_batch_schema()
        ds.write_dataset(
            batch_iterator,
            schema=schema,
            existing_data_behavior='delete_matching',
            basename_template=f'{uuid4()}-{{i}}.parquet',
            **write_kwargs
        )
    else:
        # Write a dataset with one file per batch
        for batch in batch_iterator:
            # write a parquet file with one row group for the current batch
            ds.write_dataset(
                batch,
                existing_data_behavior='overwrite_or_ignore',
                basename_template=f'{uuid4()}-{{i}}.parquet',
                **write_kwargs
            )


@click.command()
@click.option('--compact/--no-compact', show_default=True, default=False,
              help='Compact dataset with only 1 file per partition')
@click.option('--out-path', help='path where the dataset is written')
@click.option('--batch-size', default=10_000, type=int, show_default=True,
              help='Number of rows in a batch')
@click.option('--n-batches', default=300, type=int, show_default=True,
              help='Number of batches to generate')
@click.option('--batch-duration', default=24, type=float, show_default=True,
              help='Batch time duration in hours')
@click.option('--verbose', is_flag=True, show_default=False, default=False,
              help='Print info for each saved file')
def main(compact, out_path, batch_size, n_batches, batch_duration, verbose):
    """
    Generate a flat parquet dataset with random data.

    The dataset contains one parquet file per batch in a flat folder
    (no partitions). It contains numeric columns, categorical columns and a
    datetime column called "datetime".
    """
    batch_iterator = batch_generator(
        batch_size=batch_size, n_batches=n_batches,
        batch_duration=pd.Timedelta(batch_duration, unit='hour')
    )
    create_dataset(out_path, batch_iterator, compact=compact, verbose=verbose)


if __name__ == '__main__':
    main(auto_envvar_prefix='ARG')

    # Test call with hardcoded args, to be used with debugger
    # main.callback(path='mini_batch_dataset', compact=False, batch_size=10,
    #               batch_duration=2, n_batches=2)
