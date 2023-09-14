from caixa_de_ferramentas.redis_client_api import RedisAPI


def delete_cached_data(**kwargs):
    contract_name = kwargs['CONTRACT_NAME']
    start_date = kwargs['START_DATE']
    redis_service = kwargs['REDIS_SERVICE']
    redis_port = kwargs['REDIS_PORT']
    redis_client = RedisAPI(host=redis_service, port=redis_port)
    name = f"{contract_name}_{start_date.replace('-', '')}"
    redis_client.delete_key(name)





