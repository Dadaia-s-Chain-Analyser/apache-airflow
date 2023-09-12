import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.apache.hive.operators.hive import HiveOperator
from scripts.python.ingest_contract_txs_to_hadoop import run_ingestor
from scripts.hive.handle_hive_tables import create_batch_transactions_table
load_dotenv()

default_args ={
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "marco_aurelio_reis@yahoo.com.br",
    "retries": 1,
    "retry_delay": timedelta(minutes=5) 
}


COMMON_PARMS = dict(
        image="marcoaureliomenezes/batch-contract-txs:latest",
        api_version='auto', 
        docker_url="unix:///var/run/docker.sock",
        network_mode='airflow-network',
        auto_remove=True,
        mount_tmp_dir=False)




with DAG(
            f"p_0_setup_environment", 
            start_date=datetime(2023,8,20, 3), 
            schedule_interval="@once", 
            default_args=default_args,
            max_active_runs=1,
            catchup=False
        ) as dag:


    starting_process = BashOperator(
        task_id="starting_task",
        bash_command="""sleep 2"""
    )

    # creating_kafka_topics = DockerOperator(
    #     task_id="creating_kafka_topics",
    #     container_name="creating_kafka_topics",
    #     entrypoint=["python", "-u", "1_get_and_cache_contract_txs.py"],
    #     depends_on_past=True,
    #     environment=dict(NETWORK = os.environ['NETWORK']),
    #     **COMMON_PARMS
    # )

    creating_hive_batch_transactions_mainnet_table = HiveOperator(
        task_id="creating_hive_batch_transactions_mainnet_table",
        hive_cli_conn_id="hive_conn",
        hql=f"""{create_batch_transactions_table('mainnet')}""",
    )

    creating_hive_batch_transactions_goerli_table = HiveOperator(
        task_id="creating_hive_batch_transactions_goerli_table",
        hive_cli_conn_id="hive_conn",
        hql=f"""{create_batch_transactions_table('goerli')}""",
    )

    end_process = BashOperator(
        task_id="end_task",
        bash_command="""sleep 2"""
    )


    starting_process >> creating_hive_batch_transactions_mainnet_table >> creating_hive_batch_transactions_goerli_table >> end_process

