#!/bin/bash

# Optional: Load HPC modules
module load release/24.10
module load Anaconda3
source $EBROOTANACONDA3/etc/profile.d/conda.sh

# Activate conda environment
conda activate /data/horse/ws/sash820h-test-corr/conda-env/stimela/

# command line arguments
INPUT_YAML=$1

# Go to directory where stimela could be run
cd /data/horse/ws/sash820h-test-corr/prod/stimela_run_dir/

# Run stimela
stimela run "$INPUT_YAML"

