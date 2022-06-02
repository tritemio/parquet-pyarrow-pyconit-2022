# %%
import pyarrow.dataset as ds    # type: ignore
import pyarrow.parquet as pq    # type: ignore
from rich import print as rprint
from rich.pretty import pprint

path = 'mini_batch_dataset/year=2020/month=7/2307a774-5a60-41a9-a6e9-06fd9c9b7d31-0.parquet'


# %%

# This is only a pointer to the dataset
dataset = ds.dataset(path)
rprint(dataset.schema)

# %%
fragment = list(dataset.get_fragments())[0]
file = dataset.files[0]
assert fragment.path == file

rprint(f'{fragment.metadata=}')

# %% [markdown]
# This dataset has only 1 row group per file

# %%
row_group = fragment.row_groups[0]

rprint(f'\n>>> {row_group.metadata=}')
rprint('\n* Row group statistics:')
pprint(row_group.statistics)

# %%
# Read methods
scanner = dataset.scanner()
batch_iterator = dataset.to_batches()
table = dataset.to_table()
df.head(5)

# %% [markdown]

# A pointer to a single parquet file

# %%
parquet_file = pq.ParquetFile(path)
rprint(f'* Parquet File Schema:\n{parquet_file.schema}\n')
rprint(f'* Parquet File Metadata:\n{parquet_file.metadata}\n')
# %%
