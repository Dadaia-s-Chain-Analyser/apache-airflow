import json, os, subprocess
from datetime import datetime as dt
from azure.identity import DefaultAzureCredential
from caixa_de_ferramentas.redis_client_api import RedisAPI
from caixa_de_ferramentas.azure_blob_api import BlobClientApi
from ingestor import Ingestor


class BlobIngestor:

    def __init__(self, redis_client, blob_client, container, contract_name, start_date):
        self.contract_name = contract_name
        self.start_date = start_date
        self.redis_client = redis_client
        self.container = container
        self.ingestor = Ingestor(redis_client)
        self.blob_client = blob_client


    def upload_to_blob(self, path, directory):
        directory = os.path.join(directory, path.split('/')[-1])
        print(f"PATH: {path}")
        print(f"DIRECTORY: {directory}")
        self.blob_client.upload_blob(self.container, file_src=path, file_dst=directory)
        subprocess.run(["rm", path])


    def write_json(self, data, path):
        with open(path, 'w') as file:
            file.write(json.dumps(data))
            

    def ingest(self, directory='/user/hadoop/'):
        odate = dt.strftime(dt.strptime(self.start_date, '%Y-%m-%d'), '%Y%m%d')
        key = self.ingestor.gen_redis_key(self.contract_name, odate)
        data = self.redis_client.get_key(key)
        if len(data) == 0: 
            print(f"Data for {key} is empty")
            return
        path = os.path.join("/tmp", self.ingestor.form_filename(data))
        self.write_json(data, path)

        self.upload_to_blob(path, directory=directory)


def store_to_azure_adls(**kwargs):
    storage_account_name = kwargs['STORAGE_ACCOUNT_NAME']
    redis_service = kwargs['REDIS_SERVICE']
    redis_port = kwargs['REDIS_PORT']
    container_name = kwargs['NETWORK']
    contract_name = kwargs['CONTRACT_NAME']
    start_date = kwargs['START_DATE']

    credential = DefaultAzureCredential()
    blob_api = BlobClientApi(storage_account_name, credential)
    redis_client = RedisAPI(host=redis_service, port=redis_port)
    blob_ingestor = BlobIngestor(redis_client, blob_api, container_name, contract_name, start_date)
    blob_ingestor.ingest(f'batch/{contract_name}')
    print(f"Successfully ingested data to blob storage container {container_name}")