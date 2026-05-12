#!/bin/bash
#SBATCH --output=/data/horse/ws/sash820h-test-corr/prod/batch_logs/output_%j.txt
#SBATCH --error=/data/horse/ws/sash820h-test-corr/prod/batch_logs/error_%j.txt
#SBATCH --error=logs/error_%j.txt
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --gres=gpu:1
#SBATCH --time=00:20:00


# Optional: Load HPC modules
module load release/24.10
module load Anaconda3
source $EBROOTANACONDA3/etc/profile.d/conda.sh

# Activate conda environment
conda activate /data/horse/ws/sash820h-test-corr/conda-env/mkat-gpu/

#Input the main text file containing all the dada file
INP_TXT_FILE=$1

# Get the input file corresponding to the task ID
INPUT_FILE=$(sed -n "$((SLURM_ARRAY_TASK_ID + 1))p" "$INP_TXT_FILE")

PROGRAM_PATH="/home/sash820h/softwares/prod_proc/standard-correlator/src/correlator.py"
META_FILE="/data/narwhal/usr/projects/p_dza/p_dza_mpifr_compact/DADA_FILES/bvrmetadata_2024-05-19T16_48_52_7121b.hdf5"
OUTDIR="/data/horse/ws/sash820h-test-corr/prod/uvh5/"

# Run your multiprocessing script
python "$PROGRAM_PATH" "$INPUT_FILE" "$META_FILE" -o "$OUTDIR" -b gpu

