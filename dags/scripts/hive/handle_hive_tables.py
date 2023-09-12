###############################################################################################################
#####################################   PUBLIC_DATA_TABLES  ###################################################

def create_online_transactions_table(network): 
    return f"""
            CREATE DATABASE IF NOT EXISTS {network};
            CREATE TABLE IF NOT EXISTS {network}.online_transactions (
                hash_key string,
                nome string,
                cpf string,
                agencia bigint,
                conta bigint,
                renda_mensal double,
                saldo double,
                senha string,
                data_nascimento string,
                data_entrada string)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
    """


def create_batch_transactions_table(network):
    return f"""
            CREATE DATABASE IF NOT EXISTS {network};
            CREATE TABLE IF NOT EXISTS {network}.batch_transactions (
                block_number bigint,
                tx_timestamp bigint,
                hash string,
                nonce int,
                block_hash string,
                transaction_index int,
                from_address string,
                to_address string,
                value string,
                gas int, 
                gas_price bigint,
                is_error int,
                txreceipt_status int,
                input string,
                contract_address string,
                cumulative_gas_used bigint,
                gas_used int,
                confirmations int,
                method_id string,
                method_name string,
                contract_alias string,
                dat_ref_carga string)
            STORED AS PARQUET
    """


def create_metadata_oracle_prices(network):
    return f"""
            CREATE DATABASE IF NOT EXISTS {network};
            CREATE TABLE IF NOT EXISTS {network}.metadata_pricefeeds (
                table_name string,
                network string,
                pair string,
                address string,
                phase_id bigint,
                decimals bigint,
                description string)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
    """


def create_oracle_pricefeeds_table(network):
    return f"""
            CREATE DATABASE IF NOT EXISTS {network};
            CREATE TABLE IF NOT EXISTS {network}.pricefeeds (
                round_id string,
                answeredInRound string,
                started_at double,
                updated_at double,
                price double)
            PARTITIONED BY(pair string, network string)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
    """


def deleting_blockchain_database(network):
    return f"""
            DROP TABLE IF EXISTS {network}.batch_transactions;
            DROP TABLE IF EXISTS {network}.online_transactions;
            DROP TABLE IF EXISTS {network}.create_metadata_oracle_prices;
            DROP TABLE IF EXISTS {network}.pricefeeds;
            DROP DATABASE IF EXISTS {network};
    """
