from datetime import datetime

from airflow.providers.ssh.operators.ssh import SSHOperator

from airflow import DAG

SSH_CONN_ID = "capella_gpu"  # must match your Airflow connection


with DAG(
    dag_id="test_ssh",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ssh", "test"],
) as dag:
    test_connection = SSHOperator(
        task_id="test_ssh_basic",
        ssh_conn_id=SSH_CONN_ID,
        command="echo 'SSH connection successful: you did it ass**'; hostname; whoami",
    )
