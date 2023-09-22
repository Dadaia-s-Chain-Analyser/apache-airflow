import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from scripts.python.pipeline_1_packages.get_data_from_chain_and_cache import get_data_from_chain_and_cache
from scripts.python.pipeline_1_packages.ingest_contract_txs_to_hadoop import store_cached_data_to_hadoop
from scripts.python.pipeline_1_packages.ingest_contract_txs_to_azure import store_to_azure_adls
from scripts.python.pipeline_1_packages.cleaning_cache import delete_cached_data

load_dotenv()

default_args ={
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'marco_aurelio_reis@yahoo.com.br',
    'retries': 1,
    'retry_delay': timedelta(minutes=5) 
}


KEY_VAULT_SCAN_NAME = "DMEtherscanAsAService"


with DAG(
            f'p_1_mainnet_batch_hist_txs', 
            start_date=datetime(2023,8,1, 3), 
            schedule_interval='@daily', 
            default_args=default_args,
            max_active_runs=1,
            catchup=True
        ) as dag:


    starting_process = BashOperator(
        task_id='starting_task',
        bash_command='''sleep 2'''
    )

    starting_process_2 = BashOperator(
        task_id='starting_process_2',
        bash_command='''sleep 2'''
    )

    get_aave_v2_txs = PythonOperator(
        task_id='get_aave_v2_txs',
        python_callable=get_data_from_chain_and_cache,
        depends_on_past=True,
        op_kwargs=dict(
                        NETWORK = 'mainnet',
                        REDIS_SERVICE = 'redis',
                        REDIS_PORT = 6379,
                        KEY_VAULT_SCAN_NAME = KEY_VAULT_SCAN_NAME,
                        KEY_VAULT_SCAN_SECRET = 'etherscan-api-key-1',
                        CONTRACT = "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9",
                        CONTRACT_NAME = 'aave_v2',
                        START_DATE = '{{ prev_ds }}',
                        END_DATE = '{{ ds }}'
        )
    )


    get_aave_v3_txs = PythonOperator(
        task_id='get_aave_v3_txs',
        python_callable=get_data_from_chain_and_cache,
        depends_on_past=True,
        op_kwargs=dict(
                        NETWORK = 'mainnet',
                        REDIS_SERVICE = 'redis',
                        REDIS_PORT = 6379,
                        KEY_VAULT_SCAN_NAME = KEY_VAULT_SCAN_NAME,
                        KEY_VAULT_SCAN_SECRET = 'etherscan-api-key-1',
                        CONTRACT = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
                        CONTRACT_NAME = 'aave_v3',
                        START_DATE = '{{ prev_ds }}',
                        END_DATE = '{{ ds }}'
        )
    )

    get_uniswap_v2_txs = PythonOperator(
        task_id='get_uniswap_v2_txs',
        python_callable=get_data_from_chain_and_cache,
        depends_on_past=True,
        op_kwargs=dict(
                        NETWORK = 'mainnet',
                        REDIS_SERVICE = 'redis',
                        REDIS_PORT = 6379,
                        KEY_VAULT_SCAN_NAME = KEY_VAULT_SCAN_NAME,
                        KEY_VAULT_SCAN_SECRET = 'etherscan-api-key-1',
                        CONTRACT = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",
                        CONTRACT_NAME = 'uniswap_v2',
                        START_DATE = '{{ prev_ds }}',
                        END_DATE = '{{ ds }}'
        )
    )

    ingest_aave_v2_txs_to_hadoop = PythonOperator(
        task_id='ingest_aave_v2_txs_to_hadoop',
        python_callable=store_cached_data_to_hadoop,
        op_kwargs=dict(
                        NETWORK='mainnet', 
                        CONTRACT_NAME='aave_v2', 
                        START_DATE='{{ prev_ds }}'
        )
    )


    ingest_aave_v3_txs_to_hadoop = PythonOperator(
        task_id='ingest_aave_v3_txs_to_hadoop',
        python_callable=store_cached_data_to_hadoop,
        op_kwargs=dict(
                        NETWORK='mainnet', 
                        CONTRACT_NAME='aave_v3', 
                        START_DATE='{{ prev_ds }}'
        ),
    )


    ingest_uniswap_v2_txs_to_hadoop = PythonOperator(
        task_id='ingest_uniswap_v2_txs_to_hadoop',
        python_callable=store_cached_data_to_hadoop,
        op_kwargs=dict(
                        NETWORK='mainnet',
                        CONTRACT_NAME='uniswap_v2',
                        START_DATE='{{ prev_ds }}'
        ),
    )


    ingest_aave_v2_txs_to_azure = PythonOperator(
        task_id='ingest_aave_v2_txs_to_azure',
        python_callable=store_to_azure_adls,
        op_kwargs=dict(
                        NETWORK = 'mainnet',
                        REDIS_SERVICE = 'redis',
                        REDIS_PORT = 6379,
                        CONTRACT_NAME = 'aave_v2',
                        START_DATE = '{{ prev_ds }}',
                        STORAGE_ACCOUNT_NAME = 'dadaiastorage',
        )
    )

    ingest_aave_v3_txs_to_azure = PythonOperator(
        task_id='ingest_aave_v3_txs_to_azure',
        python_callable=store_to_azure_adls,
        op_kwargs=dict(
                        NETWORK = 'mainnet',
                        REDIS_SERVICE = 'redis',
                        REDIS_PORT = 6379,
                        CONTRACT_NAME = 'aave_v3',
                        START_DATE = '{{ prev_ds }}',
                        STORAGE_ACCOUNT_NAME = 'dadaiastorage',
        )
    )

    ingest_uniswap_v2_txs_to_azure = PythonOperator(
        task_id='ingest_uniswap_v2_txs_to_azure',
        python_callable=store_to_azure_adls,
        op_kwargs=dict(
                        NETWORK = 'mainnet',
                        REDIS_SERVICE = 'redis',
                        REDIS_PORT = 6379,
                        CONTRACT_NAME = 'uniswap_v2',
                        START_DATE = '{{ prev_ds }}',
                        STORAGE_ACCOUNT_NAME = 'dadaiastorage',
        )
    )


    delete_aave_v2_cache = PythonOperator(
        task_id='delete_aave_v2_cache',
        python_callable=delete_cached_data,
        op_kwargs=dict(
                        REDIS_SERVICE = 'redis',
                        REDIS_PORT = 6379,
                        CONTRACT_NAME = 'aave_v2',
                        START_DATE = '{{ prev_ds }}'
        )
    )


    delete_aave_v3_cache = PythonOperator(
        task_id='delete_aave_v3_cache',
        python_callable=delete_cached_data,
        op_kwargs=dict(
                        REDIS_SERVICE = 'redis',
                        REDIS_PORT = 6379,
                        CONTRACT_NAME = 'aave_v3',
                        START_DATE = '{{ prev_ds }}'

        )
    )


    delete_uniswap_v2_cache = PythonOperator(
        task_id='delete_uniswap_v2_cache',
        python_callable=delete_cached_data,
        op_kwargs=dict(
                        REDIS_SERVICE = 'redis',
                        REDIS_PORT = 6379,
                        CONTRACT_NAME = 'uniswap_v2',
                        START_DATE = '{{ prev_ds }}'
        )
    )

    # Passing execution date as parameter to the spark job
    add_partition_aave_v2_txs_table = SparkSubmitOperator(
        task_id='add_partition_aave_v2_txs_table',
        conn_id='spark_conn',
        application='/opt/airflow/dags/scripts/spark/transactions_processing.py',
        verbose=False,
        conf={'spark.driver.memory': '10G'},
        application_args=['mainnet', 'aave_v2', '{{ ds }}']
    )

    # Passing execution date as parameter to the spark job
    add_partition_aave_v3_txs_table = SparkSubmitOperator(
        task_id='add_partition_aave_v3_txs_table',
        conn_id='spark_conn',
        application='/opt/airflow/dags/scripts/spark/transactions_processing.py',
        verbose=False,
        conf={'spark.driver.memory': '10G'},
        application_args=['mainnet', 'aave_v3', '{{ ds }}']
    )

    # Passing execution date as parameter to the spark job
    add_partition_uniswap_v2_txs_table = SparkSubmitOperator(
        task_id='add_partition_uniswap_v2_txs_table',
        conn_id='spark_conn',
        application='/opt/airflow/dags/scripts/spark/transactions_processing.py',
        verbose=False,
        conf={'spark.driver.memory': '10G'},
        application_args=['mainnet', 'uniswap_v2', '{{ ds }}']
    )

    end_task = BashOperator(
        task_id='end_task',
        bash_command='''sleep 2'''
    )
    # get_aave_v3_txs = DockerOperator(
    #     task_id='get_aave_v3_txs',
    #     container_name='get_aave_v3_txs',
    #     entrypoint=['python', '1_get_and_cache_contract_txs.py', '--start_date', start_date, '--end_date', end_date],
    #     environment=dict(
    #                     NETWORK = 'mainnet',
    #                     AZURE_CLIENT_ID = os.environ['AZURE_CLIENT_ID'],
    #                     AZURE_TENANT_ID = os.environ['AZURE_TENANT_ID'],
    #                     AZURE_CLIENT_SECRET = os.environ['AZURE_CLIENT_SECRET'],
    #                     KEY_VAULT_SCAN_NAME = os.environ['KEY_VAULT_SCAN_NAME'],
    #                     KEY_VAULT_SCAN_SECRET = 'etherscan-api-key-2',
    #                     CONTRACT = os.environ['MAINNET_AAVE_V3_POOL']),
    #     **COMMON_PARMS
    # )


    # ingest_aave_v3_txs_to_azure = DockerOperator(
    #     task_id='ingest_aave_v3_txs_to_azure',
    #     container_name='ingest_aave_v3_txs_to_azure',
    #     entrypoint=['python', '-u', '3_ingest_contract_txs_to_adls.py'],
    #     environment=dict(
    #                     NETWORK = 'mainnet',
    #                     AZURE_CLIENT_ID = os.environ['AZURE_CLIENT_ID'],
    #                     AZURE_TENANT_ID = os.environ['AZURE_TENANT_ID'],
    #                     AZURE_CLIENT_SECRET = os.environ['AZURE_CLIENT_SECRET']),
    #     **COMMON_PARMS
    # )

    # get_uniswap_v2_txs = DockerOperator(
    #     task_id='get_uniswap_v2_txs',
    #     container_name='get_uniswap_v2_txs',
    #     entrypoint=['python', '-u', '5_batch_contract_transactions.py', '--start_date', '2022-01-01'],
    #     environment=dict(
    #                     NETWORK = 'mainnet',
    #                     AZURE_CLIENT_ID = os.environ['AZURE_CLIENT_ID'],
    #                     AZURE_TENANT_ID = os.environ['AZURE_TENANT_ID'],
    #                     AZURE_CLIENT_SECRET = os.environ['AZURE_CLIENT_SECRET'],
    #                     KEY_VAULT_SCAN_NAME = os.environ['KEY_VAULT_SCAN_NAME'],
    #                     KEY_VAULT_SCAN_SECRET = 'etherscan-api-key-1',
    #                     KAFKA_ENDPOINT = os.environ['KAFKA_ENDPOINT'],
    #                     TOPIC_PRODUCE = 'batch_uniswap_v2_txs',
    #                     CONTRACT = os.environ['MAINNET_UNISWAP_V2_ROUTER_02']),
    #     **COMMON_PARMS
    # )

    # ingest_uniswap_v2_txs_to_azure = DockerOperator(
    #     task_id='ingest_uniswap_v2_txs_to_azure',
    #     container_name='ingest_uniswap_v2_txs_to_azure',
    #     entrypoint=['python', '-u', '3_ingest_contract_txs_to_adls.py'],
    #     environment=dict(
    #                     NETWORK = 'mainnet',
    #                     AZURE_CLIENT_ID = os.environ['AZURE_CLIENT_ID'],
    #                     AZURE_TENANT_ID = os.environ['AZURE_TENANT_ID'],
    #                     AZURE_CLIENT_SECRET = os.environ['AZURE_CLIENT_SECRET']),
    #     **COMMON_PARMS
    # )


    # get_uniswap_v3_txs = DockerOperator(
    #     task_id='get_uniswap_v3_txs',
    #     container_name='get_uniswap_v3_txs',
    #     entrypoint=['python', '-u', '5_batch_contract_transactions.py', '--start_date', '2022-01-01'],
    #     environment=dict(
    #                     NETWORK = 'mainnet',
    #                     AZURE_CLIENT_ID = os.environ['AZURE_CLIENT_ID'],
    #                     AZURE_TENANT_ID = os.environ['AZURE_TENANT_ID'],
    #                     AZURE_CLIENT_SECRET = os.environ['AZURE_CLIENT_SECRET'],
    #                     KEY_VAULT_SCAN_NAME = os.environ['KEY_VAULT_SCAN_NAME'],
    #                     KEY_VAULT_SCAN_SECRET = 'etherscan-api-key-2',
    #                     KAFKA_ENDPOINT = os.environ['KAFKA_ENDPOINT'],
    #                     TOPIC_PRODUCE = 'batch_uniswap_v3_txs',
    #                     CONTRACT = os.environ['MAINNET_UNISWAP_V3_ROUTER']),
    #     **COMMON_PARMS
    # )


    # ingest_uniswap_v3_txs_to_azure = DockerOperator(
    #     task_id='ingest_uniswap_v3_txs_to_azure',
    #     container_name='ingest_uniswap_v3_txs_to_azure',
    #     entrypoint=['python', '-u', '3_ingest_contract_txs_to_adls.py'],
    #     environment=dict(
    #                     NETWORK = 'mainnet',
    #                     AZURE_CLIENT_ID = os.environ['AZURE_CLIENT_ID'],
    #                     AZURE_TENANT_ID = os.environ['AZURE_TENANT_ID'],
    #                     AZURE_CLIENT_SECRET = os.environ['AZURE_CLIENT_SECRET']),
    #     **COMMON_PARMS
    # )

    # decode_uniswap_v2_input_txs = DockerOperator(
    #     task_id='decode_uniswap_v2_input_txs',
    #     container_name='decode_uniswap_v2_input_txs',
    #     entrypoint=f'python tx_input_converters.py contract_address={UNISWAP_V2_ADDRESS} auto_offset_reset=earliest'.split(' '),
    #     environment=dict(
    #                     NETWORK = 'mainnet',  
    #                     KAFKA_HOST = os.env,
    #                     TOPIC_INPUT = 'uniswap_v2_txs',
    #                     TOPIC_OUTPUT = 'inputs_uniswap_v2_txs',
    #                     CONSUMER_GROUP = 'uniswap-v2-decoders',
    #                     SCAN_API_KEY = os.environ['SCAN_API_KEY1'],
    #                     NODE_API_KEY = os.environ['INFURA_API_KEY16']),
    #     **COMMON_PARMS
    # )

    # decode_uniswap_v3_input_txs = DockerOperator(
    #     task_id='decode_uniswap_v3_input_txs',
    #     container_name='decode_uniswap_v3_input_txs',
    #     entrypoint=f'python tx_input_converters.py contract_address={UNISWAP_V3_ADDRESS} auto_offset_reset=earliest'.split(' '),
    #     environment=dict(
    #                     NETWORK = 'mainnet', 
    #                     KAFKA_HOST = os.env,
    #                     TOPIC_INPUT = 'uniswap_v3_txs',
    #                     TOPIC_OUTPUT = 'inputs_uniswap_v3_txs',
    #                     CONSUMER_GROUP = 'uniswap-v3-decoders',
    #                     SCAN_API_KEY = os.environ['SCAN_API_KEY1'],
    #                     NODE_API_KEY = os.environ['INFURA_API_KEY16']),
    #     **COMMON_PARMS
    # )




    starting_process >> starting_process_2 >> get_aave_v2_txs >> [ingest_aave_v2_txs_to_hadoop, ingest_aave_v2_txs_to_azure] >> delete_aave_v2_cache
    starting_process >> starting_process_2 >> get_aave_v3_txs >> [ingest_aave_v3_txs_to_hadoop, ingest_aave_v3_txs_to_azure] >> delete_aave_v3_cache
    starting_process >> starting_process_2 >> get_uniswap_v2_txs >> [ingest_uniswap_v2_txs_to_hadoop, ingest_uniswap_v2_txs_to_azure] >> delete_uniswap_v2_cache
    [ delete_aave_v2_cache, delete_aave_v3_cache, delete_uniswap_v2_cache ] >> add_partition_aave_v2_txs_table 
    add_partition_aave_v2_txs_table >> add_partition_aave_v3_txs_table >> add_partition_uniswap_v2_txs_table >> end_task
    #starting_process >> get_uniswap_v2_txs
    #starting_process >> get_uniswap_v3_txs
