#!/bin/bash
#SBATCH --job-name=concatenate-uvh5
#SBATCH --output=/data/horse/ws/sash820h-test-corr/prod/batch_logs/output_%j.txt
#SBATCH --error=/data/horse/ws/sash820h-test-corr/prod/batch_logs/error_%j.txt
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=32G
#SBATCH --time=00:20:00

# Optional: Load HPC modules
module load release/24.10
module load Anaconda3
source $EBROOTANACONDA3/etc/profile.d/conda.sh

# Activate conda environment
conda activate /data/horse/ws/sash820h-test-corr/conda-env/mkat/

# command line arguments
INPUT=$1

# Runs file-finder from all spectral folders
python  /home/sash820h/softwares/std-img-pipeline/util_scripts/combine_uvh5.py $INPUT