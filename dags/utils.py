import json
import re
import time

from airflow.providers.ssh.hooks.ssh import SSHHook


def parse_job_id(output: str) -> int:
    return re.search(r"\d+", output).group(0)


def execute_bash(command: str, ssh_conn_id: str) -> str:
    """
    Executes a bash command on remote machine
    Inputs:
    command: shell command
    ssh_conn_id: Id of the remote machine
    """
    # ssh connection
    hook = SSHHook(ssh_conn_id=ssh_conn_id)
    client = hook.get_conn()

    # execution outputs from remote machine
    stdin, stdout, stderr = client.exec_command(command)
    output = stdout.read().decode()
    return output


def execute_bash_file_collector(command: str, ssh_conn_id: str) -> str:
    """
    Executes a bash command on remote machine for file collection
    Inputs:
    command: shell command
    ssh_conn_id: Id of the remote machine
    """
    hook = SSHHook(ssh_conn_id=ssh_conn_id)
    client = hook.get_conn()

    stdin, stdout, stderr = client.exec_command(command)
    output = stdout.read().decode()
    try:
        data = json.loads(output)
    except json.JSONDecodeError as e:
        raise Exception(f"Invalid JSON: {output}") from e

    file_info = data["file_meta"]

    return file_info


def submit_slurm_job(command: str, ssh_conn_id: str) -> int:
    """
    Executes a SLURM job on remote machine
    Inputs:
    command: shell command
    ssh_conn_id: Id of the remote machine
    """

    hook = SSHHook(ssh_conn_id=ssh_conn_id)
    client = hook.get_conn()

    stdin, stdout, stderr = client.exec_command(command)
    output = stdout.read().decode()
    job_id = parse_job_id(output)
    return job_id


def wait_for_slurm_job(job_id: int, ssh_conn_id: str, poll_interval: int = 120) -> None:
    """
    Check the status of the status of slurm job regularly
    Inputs:
    job_id: Slurm job id
    ssh_conn_id: Id of the remote machine
    poll_interval: retry interval for status
    """

    hook = SSHHook(ssh_conn_id=ssh_conn_id)
    client = hook.get_conn()

    while True:
        cmd = f"sacct -j {job_id} --format=JobID,State --parsable2 --noheader |grep -v batch | grep -v extern"
        _, stdout, stderr = client.exec_command(cmd)

        status = stdout.read().decode().strip()
        error = stderr.read().decode()

        if error:
            raise Exception(f"Slurm error: {error}")

        if not status:
            print("No job info yet, retrying...")
            time.sleep(poll_interval)
            continue

        if "COMPLETED" in status:
            return
        elif any(x in status for x in ["FAILED", "CANCELLED", "TIMEOUT", "OOM"]):
            raise Exception(f"Job {job_id} failed: {status}")

        time.sleep(poll_interval)


def wait_for_slurm_array(
    job_id: int, ssh_conn_id: str, poll_interval: int = 120
) -> None:
    """
    Monitor the status of the status of the slurm array job regularly
    and raise exeptions if failed.
    Inputs:
    job_id: Slurm job id
    ssh_conn_id: Id of the remote machine
    poll_interval: retry interval for status
    """
    hook = SSHHook(ssh_conn_id=ssh_conn_id)
    client = hook.get_conn()

    while True:
        cmd = f"sacct -j {job_id} --format=JobID,State --parsable2 --noheader |grep -v batch | grep -v extern"

        _, stdout, stderr = client.exec_command(cmd)

        output = stdout.read().decode().strip()
        error = stderr.read().decode()

        if error:
            raise Exception(f"Slurm error: {error}")

        if not output:
            print("No job info yet, retrying...")
            time.sleep(poll_interval)
            continue

        states = []
        for line in output.splitlines():
            parts = line.split("|")
            if len(parts) < 2:
                continue

            job, state = parts[0], parts[1]

            # Ignore parent job line (e.g., 12345)
            if "_" not in job:
                continue

            states.append(state)

        print(f"States: {states}")

        if not states:
            time.sleep(poll_interval)
            continue

        # Failure condition
        if any(s in ["FAILED", "CANCELLED", "TIMEOUT", "OOM"] for s in states):
            raise Exception(f"Job array {job_id} failed: {states}")

        # Success condition
        if all(s == "COMPLETED" for s in states):
            print(f"Job array {job_id} completed successfully")
            return

        # Still running
        time.sleep(poll_interval)


def wait_for_stimela_job(ssh_conn_id: str, logs: str) -> None:
    """
    Check the status of the different SLURM jobs submitted through stimela
    Inputs:
    ssh_conn_id: Id of the remote machine
    logs: Stimela output log
    """
    job_ids = re.findall(r"srun: job (\d+) queued and waiting for resources", logs)
    job_ids = list(map(int, job_ids))

    if job_ids:
        hook = SSHHook(ssh_conn_id=ssh_conn_id)
        client = hook.get_conn()
        states = []

        for job_id in job_ids:
            cmd = f"sacct -j {job_id} --format=JobID,State --parsable2 --noheader |grep -v batch | grep -v extern"
            _, stdout, stderr = client.exec_command(cmd)

            status = stdout.read().decode().strip()
            error = stderr.read().decode()

            if error:
                raise Exception(f"Stimela Slurm error for {job_id}: {error}")

            states.append(status)

        if all(["COMPLETED" in s for s in states]):
            print("All the Stimela associated SLURM jobs are COMPLETED")
            return

    else:
        raise Exception(
            "Stimela instance does not have associated SLURM jobs submitted"
        )
