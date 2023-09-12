import os
from dotenv import load_dotenv
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.operators.email import EmailOperator

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
        image="marcoaureliomenezes/offchain-watchers:latest",
        api_version='auto', 
        docker_url="unix:///var/run/docker.sock",
        network_mode='airflow-network',
        auto_remove=True,
        mount_tmp_dir=False)


with DAG(
            f"p_2_{os.environ['NETWORK']}_streaming_txs", 
            start_date=datetime(2021,1,1), 
            schedule_interval="@once", 
            default_args=default_args, 
            catchup=False
        ) as dag:

    start_job = BashOperator(
        task_id="start_job",
        bash_command="""sleep 2"""
    )


    stream_blocks_and_txs = DockerOperator(
        task_id="stream_blocks_and_txs",
        container_name="stream_blocks_and_txs",
        entrypoint=["python", "-u", "1_block_clock.py", "--tx_threshold", "120", "--num_partitions", "12"],
        environment=dict(
                    NETWORK = os.environ['NETWORK'],
                    AZURE_CLIENT_ID = os.environ['AZURE_CLIENT_ID'],
                    AZURE_TENANT_ID = os.environ['AZURE_TENANT_ID'],
                    AZURE_CLIENT_SECRET = os.environ['AZURE_CLIENT_SECRET'],
                    KEY_VAULT_NODE_NAME = os.environ['KEY_VAULT_NODE_NAME'],
                    KEY_VAULT_NODE_SECRET = 'alchemy-api-key-1',
                    KAFKA_ENDPOINT = os.environ["KAFKA_ENDPOINT"]),
        **COMMON_PARMS)


    with TaskGroup(group_id="process_txs") as process_txs:

        detail_tx_1 = DockerOperator(
            task_id="detail_tx_1",
            container_name="detail_tx_1",
            entrypoint=["python", "-u", "2_raw_transactions.py", "--num_partitions", "12"],
            environment=dict(
                    NETWORK = os.environ['NETWORK'], 
                    AZURE_CLIENT_ID = os.environ["AZURE_CLIENT_ID"],
                    AZURE_TENANT_ID = os.environ["AZURE_TENANT_ID"],
                    AZURE_CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"],
                    KEY_VAULT_NODE_NAME = os.environ["KEY_VAULT_NODE_NAME"],
                    KEY_VAULT_NODE_SECRET = 'infura-api-key-1-4',
                    KAFKA_ENDPOINT = os.environ["KAFKA_ENDPOINT"]),
            **COMMON_PARMS
            )

        detail_tx_2 = DockerOperator(
                task_id="detail_tx_2",
                container_name="detail_tx_2",
                entrypoint=["python", "-u", "2_raw_transactions.py", "--num_partitions", "12"],
                environment=dict(
                    NETWORK = os.environ['NETWORK'], 
                    AZURE_CLIENT_ID = os.environ["AZURE_CLIENT_ID"],
                    AZURE_TENANT_ID = os.environ["AZURE_TENANT_ID"],
                    AZURE_CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"],
                    KEY_VAULT_NODE_NAME = os.environ["KEY_VAULT_NODE_NAME"],
                    KEY_VAULT_NODE_SECRET = 'infura-api-key-5-8',
                    KAFKA_ENDPOINT = os.environ["KAFKA_ENDPOINT"]),
                **COMMON_PARMS
        )


        detail_tx_3 = DockerOperator(
                task_id="detail_tx_3",
                container_name="detail_tx_3",
                entrypoint=["python", "-u", "2_raw_transactions.py", "--num_partitions", "12"],
                environment=dict(
                    NETWORK = os.environ['NETWORK'], 
                    AZURE_CLIENT_ID = os.environ["AZURE_CLIENT_ID"],
                    AZURE_TENANT_ID = os.environ["AZURE_TENANT_ID"],
                    AZURE_CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"],
                    KEY_VAULT_NODE_NAME = os.environ["KEY_VAULT_NODE_NAME"],
                    KEY_VAULT_NODE_SECRET = 'infura-api-key-9-12',
                    KAFKA_ENDPOINT = os.environ["KAFKA_ENDPOINT"]),
                **COMMON_PARMS
        )

    classify_txs = DockerOperator(
        task_id="classify_txs",
        container_name="classify_txs",
        entrypoint=["python", "-u", "3_transaction_classifier.py"],
        environment=dict(
                    NETWORK = os.environ["NETWORK"],
                    KAFKA_ENDPOINT = os.environ["KAFKA_ENDPOINT"]
        ),
        **COMMON_PARMS)



    streaming_blocks_and_txs_alarm = EmailOperator(
        trigger_rule=TriggerRule.ONE_FAILED,
        task_id="streaming_blocks_and_txs_alarm",
        to="marcoaurelio1menezes@gmail.com",
        subject="Problem with streaming blocks and transactions job",
        html_content="<h3>An error has occurred with streaming blocks and transactions job</h3>"
    )

    streaming_transactions_alarm = EmailOperator(
        trigger_rule=TriggerRule.ONE_FAILED,
        task_id="streaming_transactions_alarm",
        to="marcoaurelio1menezes@gmail.com",
        subject="Problem with streaming raw transaction job",
        html_content="<h3>An error has occurred  with streaming raw transaction job</h3>"
    )

    classify_txs_alarm = EmailOperator(
        trigger_rule=TriggerRule.ONE_FAILED,
        task_id="classify_txs_alarm",
        to="marcoaurelio1menezes@gmail.com",
        subject="Problem with classify transactions job",
        html_content="<h3>An error has occurred with classify transactions job</h3>"
    )


    
    start_job >> [stream_blocks_and_txs, process_txs, classify_txs]
    stream_blocks_and_txs >> streaming_blocks_and_txs_alarm
    process_txs >> streaming_transactions_alarm
    classify_txs >> classify_txs_alarm