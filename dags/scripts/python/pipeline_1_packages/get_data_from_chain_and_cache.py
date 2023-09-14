import os, logging
from datetime import datetime
from azure.identity import DefaultAzureCredential
from ingestor import Ingestor
from caixa_de_ferramentas.redis_client_api import RedisAPI
from caixa_de_ferramentas.azure_key_vault_api import KeyVaultAPI
from caixa_de_ferramentas.etherscan_api import EthercanAPI


class BatchContractTransactions:

    def __init__(self, redis_client, api_key, network, contract_address, contract_name):
        self.contract_address = contract_address
        self.contract_name = contract_name
        self.etherscan_api = EthercanAPI(api_key, network)
        self.ingestor = Ingestor(redis_client)
        self.redis_client = redis_client


    def get_block_interval(self, start_date, end_date=None):
        start_date, end_date = int(datetime.timestamp(start_date)),  int(datetime.timestamp(end_date))
        url_start_block = self.etherscan_api.get_block_by_time_url(timestamp=start_date, closest='after')
        url_end_block = self.etherscan_api.get_block_by_time_url(timestamp=end_date, closest='before')
        block_bottom = self.etherscan_api.req_chain_scan(url_start_block)
        block_top = self.etherscan_api.req_chain_scan(url_end_block)
        return int(block_bottom), int(block_top)


    def __get_transactions(self, startblock, endblock, batch_size):
        url_start_block = self.etherscan_api.get_txlist_url(self.contract_address, startblock, endblock, offset=batch_size)
        list_transactions = self.etherscan_api.req_chain_scan(url_start_block)
        return list_transactions
    
  
    def open_channel_txs(self, start_date, end_date):
        block_bottom, block_top = self.get_block_interval(start_date, end_date=end_date)
        for data in self.batch_contract_txs(block_bottom, block_top):
            yield data


    def batch_contract_txs(self, block_bottom, block_top, batch_size=1000):
        latest_timestamp_aux = 0
        while 1:
            list_gross_tx = self.__get_transactions(block_bottom, block_top, batch_size)
            latest_timestamp = int(list_gross_tx[-1]['timeStamp'])
            if latest_timestamp_aux == latest_timestamp: break
            latest_timestamp_aux = latest_timestamp
            logging.info(f"Blocks to be analysed: {block_top - block_bottom}")
            next_bottom = int(list_gross_tx[-1]['blockNumber'])
            block_bottom = next_bottom
            if len(list_gross_tx) <= 1: break
            yield list_gross_tx
            if list_gross_tx == False: return "COMPLETED"
        return "COMPLETED"
               

    def cache_to_redis(self, start_date, data):
        odate = start_date.strftime('%Y%m%d')
        path = self.ingestor.gen_redis_key(self.contract_name, odate)
        self.redis_client.insert_key(path, data)


def get_data_from_chain_and_cache(**kwargs):

    key_vault_scan_name = kwargs['KEY_VAULT_SCAN_NAME']
    key_vault_scan_secret = kwargs['KEY_VAULT_SCAN_SECRET']
    network = kwargs['NETWORK']
    contract_address = kwargs['CONTRACT']
    contract_name = kwargs['CONTRACT_NAME']
    start_date = kwargs['START_DATE']
    end_date = kwargs['END_DATE']

    redis_client = RedisAPI(host='redis', port=6379)
    credential = DefaultAzureCredential()
    key_vault_api = KeyVaultAPI(key_vault_scan_name, credential)
    api_key = key_vault_api.get_secret(key_vault_scan_secret)

    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d') if end_date else datetime.now()
    batch_contract_txs = BatchContractTransactions(redis_client, api_key, network, contract_address, contract_name)
    timestamp_to_formatted_date = lambda timestamp: datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d %H:%M:%S')

    for transaction_page in batch_contract_txs.open_channel_txs(start_date, end_date):
        batch_contract_txs.cache_to_redis(start_date, transaction_page)
        expected_interval = (start_date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S'))
        real_interval = (timestamp_to_formatted_date(transaction_page[0]['timeStamp']),
                            timestamp_to_formatted_date(transaction_page[-1]['timeStamp']))
        print(f"START DATE: {expected_interval[0]}")
        print(f"END DATE: {expected_interval[1]}")
        print(f"DATETIME FIRST TX: {real_interval[0]}")
        print(f"DATETIME LAST TX: {real_interval[1]}")

