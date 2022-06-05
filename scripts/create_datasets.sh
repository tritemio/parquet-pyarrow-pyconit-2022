#!/bin/bash
#
# Generate 3 parquet datasets: raw, ingestioed, compacted.
# See README.md for details
#

set -euo pipefail

# Choose one set of variables below to choose the dataset size and options

# Large dataset (~30GB)  -  does not fit in RAM
# PREFIX="large2"
# export ARG_N_BATCHES=3000
# export ARG_BATCH_DURATION=6
# export ARG_BATCH_SIZE=100000
# export ARG_OUT_PATH=${PREFIX}_raw_dataset

# Medium dataset (~3GB)  -  fits in RAM
# PREFIX="medium2"
# export ARG_N_BATCHES=300
# export ARG_BATCH_DURATION=24
# export ARG_BATCH_SIZE=100000
# export ARG_OUT_PATH=${PREFIX}_raw_dataset

# Tiny dataset (10 files)
PREFIX="tiny"
export ARG_N_BATCHES=10
export ARG_BATCH_DURATION=240
export ARG_BATCH_SIZE=100
export ARG_OUT_PATH=${PREFIX}_raw_dataset

date
env | grep -e "^ARG"

# Go to the dir contaiing the script
DIR=$(dirname "$(readlink -f "$0")")
cd "$DIR"

# Stop if failing type-hint checks
mypy data_gen.py
mypy ingestion.py
mypy compaction.py

# Create raw dataset
# NOTE: data_gen reads its arguments from the env variables set above
python data_gen.py --no-compact # --verbose
echo -e "\n$ARG_OUT_PATH"
du -sh $ARG_OUT_PATH

# Create ingested dataset: partitioned, one file per batch
date
python ingestion.py --in-path ${PREFIX}_raw_dataset --out-path ${PREFIX}_ingested_dataset
echo -e "\n${PREFIX}_ingested_dataset"
du -sh ${PREFIX}_ingested_dataset

# Create compacted dataset: partitioned, one file per partition
date
python compaction.py --in-path ${PREFIX}_ingested_dataset --out-path ${PREFIX}_compacted_dataset
echo -e "\n${PREFIX}_compacted_dataset"
du -sh ${PREFIX}_compacted_dataset

date
