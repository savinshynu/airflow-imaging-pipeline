"""
The main Python script defining all the Airflow tasks
and DAGs.
"""

import os
from datetime import datetime, timedelta
from textwrap import dedent

from airflow.decorators import task
from utils import (
    execute_bash,
    execute_bash_file_collector,
    submit_slurm_job,
    wait_for_slurm_array,
    wait_for_slurm_job,
    wait_for_stimela_job,
)

from airflow import DAG

# These directories are located locally and should be mounted for docker so that
# the dag can find the file path.
# The location in the container
input_file = "/opt/airflow/data_files/inp_file.txt"
processed_file = "/opt/airflow/data_files/proc_file.txt"

# SSH connection ids
SSH_CONN_CPU = "barnard_cpu"  # BARNARD machine
SSH_CONN_GPU = "capella_gpu"  # CAPELLA machine

# Directory in HPC worspace "horse" hosting the CASA MS files
ms_path = "/data/horse/ws/sash820h-test-corr/prod/ms/"

with open(input_file, "r") as fh:
    inp_files = [filename.rstrip() for filename in fh.readlines()]

with open(processed_file, "r") as fp:
    proc_files = [filename.rstrip() for filename in fp.readlines()]


@task(
    retries=3,
    retry_delay=timedelta(minutes=10),
    doc_md="""
      ### DADA filename collector 
      For a given timestamp, collect the dada filenames from 64 spectral folders
      and write them into a single text file which fed into Slurm array jobs
      Outputs:
      - filepath of the text file containing .dada files
      """,
)
def get_files(num: int = 1) -> str:

    file_timestamps = []
    it = 0
    for file_name in inp_files:
        if file_name not in proc_files and it < num:
            file_timestamps.append(file_name)
            it += 1
        else:
            break
    print(file_timestamps)

    cmd = f"bash /home/sash820h/softwares/std-img-pipeline/scripts/file_finder.sh {' '.join(file_timestamps)}"
    out_dict = execute_bash_file_collector(cmd, SSH_CONN_CPU)
    print(out_dict)
    return out_dict


@task(
    retries=3,
    retry_delay=timedelta(minutes=10),
    doc_md="""
      ### Correlator SLURM array job
      Takes the input text file and submit a SLURM array job in the GPU cluster)
      Input: filepath of text file
      Ouputs: UVH5 files in the shared workspace, but returns the same text file for the next task
      """,
)
def run_correlation(file_path: str, num_files: int) -> str:

    cmd = f"sbatch --array=0-{num_files - 1}%3 /home/sash820h/softwares/std-img-pipeline/scripts/correlator_batch_pipe.sh  {file_path}"
    job_id = submit_slurm_job(cmd, SSH_CONN_GPU)

    # check the status of submitted slurm jobs and returns when all the jobs are successfull.
    wait_for_slurm_array(job_id, SSH_CONN_GPU)
    return file_path


@task(
    retries=3,
    retry_delay=timedelta(minutes=10),
    doc_md="""
      ### UVH5 concatenator
      Concatenates the uvh5 files across multiple frequencies and write out CASA MS.
      Input:  filepath
      Output:  CASA MS on shared workspace, returns the same filepath locally.
      """,
)
def run_concatenation(file_path: str) -> str:

    cmd = f"sbatch /home/sash820h/softwares/std-img-pipeline/scripts/concatenate.sh  {file_path}"
    # Need a logic to return the combined measurement set as well here
    job_id = submit_slurm_job(cmd, SSH_CONN_CPU)

    wait_for_slurm_job(job_id, SSH_CONN_CPU)
    return file_path


@task(
    retries=3,
    retry_delay=timedelta(minutes=10),
    doc_md="""
      ### CASA MS verifier
      Verify if the CASA MS file exists on the expected location
      Input: text filepath
      Output: CASA MS filepath
      """,
)
def verify_output_ms(file_path: str) -> str:
    txt_file = file_path
    ms_file = (
        ms_path
        + os.path.splitext(os.path.basename(txt_file))[0].replace(":", "-")
        + ".ms"
    )
    cmd = (
        f"bash /home/sash820h/softwares/std-img-pipeline/scripts/ms_verify.sh {ms_file}"
    )
    result = execute_bash(cmd, SSH_CONN_CPU)
    print(result)
    if result.strip() == "Success":
        print("Succesfull creation of CASA MS file")
        return ms_file
    else:
        raise Exception(f"Missing output: {ms_file}")


@task(
    retries=3,
    retry_delay=timedelta(minutes=10),
    doc_md="""
      ### Stimela config updater
      Update the stimela configuration file
      Input: CASA MS filepath
      Output:  Creates a new yaml file and outputs the filepath
      """,
)
def update_yaml(ms_file: str) -> str:

    out_name = os.path.splitext(os.path.basename(ms_file))[0]
    log_name = out_name.split("_")[-1]
    print(ms_file, out_name, log_name)
    cmd = f"bash /home/sash820h/softwares/std-img-pipeline/scripts/update_yaml.sh {ms_file} {out_name} {log_name}"
    yaml_file = execute_bash(cmd, SSH_CONN_CPU)
    return yaml_file


@task(
    retries=3,
    retry_delay=timedelta(minutes=10),
    doc_md="""
      ### Stimela Executor
      Execute the stimela pipeline on the HPC Cluster
      Input: yaml filepath
      Output: Stimela logs
      """,
)
def run_stimela(yaml_file: str) -> str:

    cmd = f"bash /home/sash820h/softwares/std-img-pipeline/scripts/stimela_execute.sh {yaml_file}"
    result_logs = execute_bash(cmd, SSH_CONN_CPU)
    wait_for_stimela_job(SSH_CONN_CPU, result_logs)
    return result_logs


with DAG(
    dag_id="proto_img_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    doc_md=dedent("""
    # End-to-end Data Processing Pipeline for COMACT Meerkat Data

    This DAG:
    1. Collect filenames (voltage .dada files) different of frequency subbands (64) for a given timeframe. Each .dada file contains 15 seconds of
    data and 8.5 MHz bandwidth.
    2. Submits jobs to GPU cluster capella which would crosscorrelate each subband of voltages and write out into uvh5 file.
    3. Submit jobs to CPU cluster for concatenating all the uvh5 files across frequencies into a single CASA MS file.
    4. Verify if the expected CASA MS files exists.
    5. Next we will operate the installed Stimela pipeline in HPC.
    6. Update the template yaml file with the information about the new created CASA MS file and the changes needed.
    7. Run Stimela with pre-planned standard flagging, calibration and imaging methods.

    ## Infrastructure:
    - Airflow run externally (locally on your machine or any machine which can access HPC via SSH)
    - Airflow utilizes SSHOPerator to interact with remote machines via SSH
    - Running bash scripts, submitting Slurm and Slurm array job happens via SSH.
    - Depending on the nature, jobs are submitted to CPU and GPU clusters.
    - Utilized the shared filessystem across the CPU and GPU clusters.

    ## Failure Handling:
    - 3 retries 10 minutes apart if the tasks are failed
    - Failed json during file collection raise exceptions
    - Erros with slurm job raise exceptions.
    - If any tasks within the pipeline fails. raise exceptions.
    - Infer the status of slurm jobs periodically, asses and move on to next tasks.
    """),
) as dag:
    # collect dada file list
    result_dict = get_files(1)

    # correlate the data
    uvh5_filepaths = run_correlation.expand_kwargs(result_dict)

    # concatenante uvh5 files create a single CASA MS dataset
    ms_filepaths = run_concatenation.expand(file_path=uvh5_filepaths)

    # check if the supposedly MS file actually exist
    comb_ms_path = verify_output_ms.expand(file_path=ms_filepaths)

    # update the existing yaml file in HPC with the new CASA MS dataset
    yaml_file_path = update_yaml.expand(ms_file=comb_ms_path)

    # Run stimela with the new yaml file
    res_logs = run_stimela.expand(yaml_file=yaml_file_path)
