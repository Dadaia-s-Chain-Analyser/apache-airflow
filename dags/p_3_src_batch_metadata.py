# import os
# from dotenv import load_dotenv
# from airflow import DAG
# from datetime import datetime, timedelta
# from airflow.operators.bash import BashOperator
# from airflow.providers.docker.operators.docker import DockerOperator
# from airflow.utils.trigger_rule import TriggerRule
# from airflow.utils.task_group import TaskGroup
# from airflow.operators.email import EmailOperator

# load_dotenv()


# default_args ={
#     "owner": "airflow",
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "email": "marco_aurelio_reis@yahoo.com.br",
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5) 
# }

# COMMON_PARMS = dict(
#         image="marcoaureliomenezes/onchain-watcher:1.0",
#         api_version='auto', 
#         docker_url="unix:///var/run/docker.sock",
#         network_mode='airflow-network',
#         auto_remove=True,
#         mount_tmp_dir=False)


# with DAG(
#     f"p_3_{os.environ['NETWORK']}_onchain_data", 
#     start_date=datetime(2021,1,1), 
#     schedule_interval="@once", 
#     default_args=default_args, catchup=False
#     ) as dag:


#     start_job = BashOperator(
#         task_id="start_job",
#         bash_command="""sleep 2"""
#     )

#     batch_aave_erc20_tokens_v2 = DockerOperator(
#         task_id="batch_aave_erc20_tokens_v2",
#         container_name="batch_aave_erc20_tokens_v2",
#         entrypoint=["brownie", "run", "scripts/1_batch_aave_erc20_tokens.py", "main", "2", "--network", os.environ['NETWORK']],
#         environment=dict(
#             AZURE_CLIENT_ID= os.environ["AZURE_CLIENT_ID"],
#             AZURE_TENANT_ID= os.environ["AZURE_TENANT_ID"],
#             AZURE_CLIENT_SECRET= os.environ["AZURE_CLIENT_SECRET"],
#             KEY_VAULT_NODE_NAME= os.environ["KEY_VAULT_NODE_NAME"],
#             KEY_VAULT_NODE_SECRET= 'infura-api-key-15',
#         ),
#         **COMMON_PARMS)


#     batch_aave_erc20_tokens_v3 = DockerOperator(
#         task_id="batch_aave_erc20_tokens_v3",
#         container_name="batch_aave_erc20_tokens_v3",
#         entrypoint=["brownie", "run", "scripts/1_batch_aave_erc20_tokens.py", "main", "3", "--network", os.environ['NETWORK']],
#         environment=dict(
#             AZURE_CLIENT_ID= os.environ["AZURE_CLIENT_ID"],
#             AZURE_TENANT_ID= os.environ["AZURE_TENANT_ID"],
#             AZURE_CLIENT_SECRET= os.environ["AZURE_CLIENT_SECRET"],
#             KEY_VAULT_NODE_NAME= os.environ["KEY_VAULT_NODE_NAME"],
#             KEY_VAULT_NODE_SECRET= 'infura-api-key-15',
#         ),
#         **COMMON_PARMS)
    
#     batch_aave_utility_tokens_v2 = DockerOperator(
#         task_id="batch_aave_utility_tokens_v2",
#         container_name="batch_aave_utility_tokens_v2",
#         entrypoint=["brownie", "run", "scripts/2_batch_aave_utility_tokens.py", "main", "2", "--network", os.environ['NETWORK']],
#         environment=dict(
#             AZURE_CLIENT_ID= os.environ["AZURE_CLIENT_ID"],
#             AZURE_TENANT_ID= os.environ["AZURE_TENANT_ID"],
#             AZURE_CLIENT_SECRET= os.environ["AZURE_CLIENT_SECRET"],
#             KEY_VAULT_NODE_NAME= os.environ["KEY_VAULT_NODE_NAME"],
#             KEY_VAULT_NODE_SECRET= 'infura-api-key-15',
#         ),
#         **COMMON_PARMS)
    
#     batch_aave_utility_tokens_v3 = DockerOperator(
#         task_id="batch_aave_utility_tokens_v3",
#         container_name="batch_aave_utility_tokens_v3",
#         entrypoint=["brownie", "run", "scripts/2_batch_aave_utility_tokens.py", "main", "2", "--network", os.environ['NETWORK']],
#         environment=dict(
#             AZURE_CLIENT_ID= os.environ["AZURE_CLIENT_ID"],
#             AZURE_TENANT_ID= os.environ["AZURE_TENANT_ID"],
#             AZURE_CLIENT_SECRET= os.environ["AZURE_CLIENT_SECRET"],
#             KEY_VAULT_NODE_NAME= os.environ["KEY_VAULT_NODE_NAME"],
#             KEY_VAULT_NODE_SECRET= 'infura-api-key-15',
#         ),
#         **COMMON_PARMS)


#     batch_aave_uniswap_v2_pair_tokens = DockerOperator(
#         task_id="batch_aave_uniswap_v2_pair_tokens",
#         container_name="batch_aave_uniswap_v2_pair_tokens",
#         entrypoint=f"brownie run scripts/batch_uniswap_pair_pools.py main 2 --network mainnet".split(" "),
#         environment=dict(
#             WEB3_INFURA_PROJECT_ID = "4dca2126970a4e6191bd6cf217e48540",
#             AZURE_CLIENT_ID= os.environ["AZURE_CLIENT_ID"],
#             AZURE_TENANT_ID= os.environ["AZURE_TENANT_ID"],
#             AZURE_CLIENT_SECRET= os.environ["AZURE_CLIENT_SECRET"],
#             KEY_VAULT_NODE_NAME= os.environ["KEY_VAULT_NODE_NAME"],
#             KEY_VAULT_NODE_SECRET= 'infura-api-key-15',
#         ),
#         **COMMON_PARMS)


#     batch_aave_uniswap_v3_pair_tokens = DockerOperator(
#         task_id="batch_aave_uniswap_v3_pair_tokens",
#         container_name="batch_aave_uniswap_v3_pair_tokens",
#         entrypoint=f"brownie run scripts/batch_uniswap_pair_pools.py main 2 --network mainnet".split(" "),
#         environment=dict(
#             WEB3_INFURA_PROJECT_ID = "4dca2126970a4e6191bd6cf217e48540",
#             AZURE_CLIENT_ID= os.environ["AZURE_CLIENT_ID"],
#             AZURE_TENANT_ID= os.environ["AZURE_TENANT_ID"],
#             AZURE_CLIENT_SECRET= os.environ["AZURE_CLIENT_SECRET"],
#             KEY_VAULT_NODE_NAME= os.environ["KEY_VAULT_NODE_NAME"],
#             KEY_VAULT_NODE_SECRET= 'infura-api-key-15',
#         ),
#         **COMMON_PARMS)

#     streaming_aave_v2_prices = DockerOperator(
#         task_id="streaming_aave_v2_prices",
#         container_name="streaming_aave_v2_prices",
#         entrypoint=f"brownie run scripts/batch_uniswap_pair_pools.py main 2 --network mainnet".split(" "),
#         environment=dict(
#             WEB3_INFURA_PROJECT_ID = "4dca2126970a4e6191bd6cf217e48540",
#             AZURE_CLIENT_ID= os.environ["AZURE_CLIENT_ID"],
#             AZURE_TENANT_ID= os.environ["AZURE_TENANT_ID"],
#             AZURE_CLIENT_SECRET= os.environ["AZURE_CLIENT_SECRET"],
#             KEY_VAULT_NODE_NAME= os.environ["KEY_VAULT_NODE_NAME"],
#             KEY_VAULT_NODE_SECRET= 'infura-api-key-15',
#         ),
#         **COMMON_PARMS)


#     streaming_aave_v3_prices = DockerOperator(
#         task_id="streaming_aave_v3_prices",
#         container_name="streaming_aave_v3_prices",
#         entrypoint=f"brownie run scripts/batch_uniswap_pair_pools.py main 2 --network mainnet".split(" "),
#         environment=dict(
#             WEB3_INFURA_PROJECT_ID = "4dca2126970a4e6191bd6cf217e48540",
#             AZURE_CLIENT_ID= os.environ["AZURE_CLIENT_ID"],
#             AZURE_TENANT_ID= os.environ["AZURE_TENANT_ID"],
#             AZURE_CLIENT_SECRET= os.environ["AZURE_CLIENT_SECRET"],
#             KEY_VAULT_NODE_NAME= os.environ["KEY_VAULT_NODE_NAME"],
#             KEY_VAULT_NODE_SECRET= 'infura-api-key-15',
#         ),
#         **COMMON_PARMS)



#     start_job >> batch_aave_erc20_tokens_v2 >> batch_aave_uniswap_v2_pair_tokens
#     start_job >> batch_aave_erc20_tokens_v2 >> streaming_aave_v2_prices
#     start_job >> batch_aave_erc20_tokens_v2 >> batch_aave_utility_tokens_v2

#     start_job >> batch_aave_erc20_tokens_v3 >> batch_aave_uniswap_v3_pair_tokens
#     start_job >> batch_aave_erc20_tokens_v3 >> streaming_aave_v3_prices
#     start_job >> batch_aave_erc20_tokens_v3 >> batch_aave_utility_tokens_v3
   