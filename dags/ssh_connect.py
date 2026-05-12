from datetime import datetime

from airflow.providers.ssh.operators.ssh import SSHOperator

from airflow import DAG

SSH_CONN_ID = "barnard"

with DAG(
    dag_id="hpc_pipeline", start_date=datetime(2024, 1, 1), schedule=None, catchup=False
) as dag:
    run_correlation = SSHOperator(
        task_id="run_gpu_job",
        ssh_conn_id=SSH_CONN_ID,
        command="hostname",
    )

    run_correlation
