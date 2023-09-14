from datetime import datetime
import subprocess, json



class Ingestor:

    def __init__(self, redis_client):
        self.redis_client = redis_client


    def form_filename(self, data):
        timestamp_start, timestamp_end = data[0]['timeStamp'], data[-1]['timeStamp']
        odate = datetime.fromtimestamp(int(timestamp_start)).strftime('%Y%m%d')
        path = f'transactions_{timestamp_start}_to_{timestamp_end}_{odate}.json'
        return path
    

    def gen_redis_key(self, contract_name, start_date):
        redis_key = f'{contract_name}_{start_date}'
        return redis_key
    