#!/bin/bash

# Optional: Load HPC modules
module load release/24.10
module load Anaconda3
source $EBROOTANACONDA3/etc/profile.d/conda.sh

# Activate conda environment
conda activate /data/horse/ws/sash820h-test-corr/conda-env/mkat/

# Collect all the command line arguments
INPUT=$@

# Runs file-finder from all spectral folders
python  /home/sash820h/softwares/std-img-pipeline/util_scripts/get_file_all_bands.py $INPUT