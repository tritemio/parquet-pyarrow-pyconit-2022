#!/bin/bash

# Large dataset (~30GB)  -  does not fit in RAM
PREFIX="large"
export ARG_N_BATCHES=30000
export ARG_BATCH_DURATION=2
export ARG_BATCH_SIZE=10000
export ARG_OUT_PATH=${PREFIX}_raw_dataset

# # Medium dataset (~3GB)  -  fits in RAM
# PREFIX="medium"
# export ARG_N_BATCHES=3000
# export ARG_BATCH_DURATION=2
# export ARG_BATCH_SIZE=10000
# export ARG_OUT_PATH=${PREFIX}_raw_dataset

# # Tiny dataset (10 files)
# PREFIX="tiny"
# export ARG_N_BATCHES=10
# export ARG_BATCH_DURATION=24
# export ARG_BATCH_SIZE=100
# export ARG_OUT_PATH=${PREFIX}_raw_dataset

python data_gen.py --no-compact # --verbose
echo -e "\n$ARG_OUT_PATH"
du -sh $ARG_OUT_PATH

# Create ingested dataset: partitioned, one file per batch
python ingestion.py --in-path ${PREFIX}_raw_dataset --out-path ${PREFIX}_ingested_dataset
echo -e "\n${PREFIX}_ingested_dataset"
du -sh ${PREFIX}_ingested_dataset

# Create compacted dataset: partitioned, one file per partition
python compaction.py --in-path ${PREFIX}_ingested_dataset --out-path ${PREFIX}_compacted_dataset
echo -e "\n${PREFIX}_compacted_dataset"
du -sh ${PREFIX}_compacted_dataset
