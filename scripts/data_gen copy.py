# %%
from uuid import uuid4
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

PATH = "sample_dataset"

# %%
def get_batch(
    n_rows: int, 
    datetime_start: pd.Timestamp, datetime_stop: pd.Timestamp,
    rng: np.random.Generator | int = 1
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
    dt_series = pd.Series(datetime_data["datetime"])
    datetime_data['year'] = dt_series.dt.year
    datetime_data['month'] = dt_series.dt.month
    datetime_data['day'] = dt_series.dt.day

    batch = pa.RecordBatch.from_pydict(
        datetime_data | categorical_data | numerical_data)
    return batch


# %%
def batch_iterator(n_batches: int, batch_size: int = 1000):
    rng = np.random.default_rng(1)

    stop = pd.Timestamp('2020-01-01')
    for _ in range(n_batches):
        start = stop
        stop += pd.Timedelta(1, unit='day')
        batch = get_batch(batch_size, start, stop, rng=rng)
        yield batch

# rng = np.random.default_rng(1)
# start = pd.Timestamp('2020-03-30')
# stop = start + pd.Timedelta(1, unit='day')
# batch = get_batch(1000, start, stop, rng=rng)

# %% [markdown]
# # Create mini-batch dataset

# %%
batch_size = 10_000
n_batches = 300

rng = np.random.default_rng(1)

format = ds.ParquetFileFormat()
write_options = format.make_write_options(
    use_dictionary=True, compression='snappy', version='2.6')
for batch in batch_iterator(n_batches=n_batches, batch_size=batch_size):
    ds.write_dataset(
        batch, 
        base_dir=PATH, 
        format=format, 
        file_options=write_options,
        partitioning=['year', 'month'], 
        partitioning_flavor='hive',
        # existing_data_behavior='overwrite_or_ignore',
        existing_data_behavior='delete_matching',
        basename_template=f'{uuid4()}-{{i}}.parquet',
        # max_rows_per_group=batch_size,
        # max_rows_per_file=batch_size,
    ) 

# %%
batch_size = 10_000
iterator = batch_iterator(n_batches=300, batch_size=batch_size)
scanner = ds.Scanner.from_batches(iterator, schema=batch.schema)


# %% [markdown]
# # Create a compacted dataset

# %%
batch_size=10_000
iterator = batch_iterator(n_batches=300, batch_size=batch_size)
scanner = ds.Scanner.from_batches(iterator, schema=batch.schema)

format = ds.ParquetFileFormat()
write_options = format.make_write_options(use_dictionary=True, compression='snappy', version='2.6')
ds.write_dataset(
    iterator, 
    schema=batch.schema,
    base_dir=PATH, 
    format=format, 
    file_options=write_options,
    partitioning=['year', 'month'], 
    partitioning_flavor='hive',
    # existing_data_behavior='overwrite_or_ignore',
    existing_data_behavior='delete_matching',
    basename_template=f'{uuid4()}-{{i}}.parquet',
    # max_rows_per_group=batch_size,
    # max_rows_per_file=batch_size,
)

# %%
dataset = ds.dataset(PATH)
df = dataset.to_table().to_pandas()
df.shape

# %%
fragments = list(dataset.get_fragments())
f = fragments[0]
f.num_row_groups

