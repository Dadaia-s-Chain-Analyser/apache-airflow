from os.path import abspath
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import  pyspark.sql.types as t


if __name__ == "__main__":

    warehouse_location = abspath('spark-warehouse')

    # Initialize Spark Session
    spark = SparkSession \
        .builder \
        .appName("Blockhain transactions ingestion") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .config("spark.driver.memory", "2G") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')


    print("Arguments: ", sys.argv[1:])
    network = sys.argv[1]
    contract = sys.argv[2]
    date = sys.argv[3]

    df_transactions = spark.read.json(f'hdfs://namenode:9000/mainnet/batch/{contract}/*')
    dict_col_names = {
        "blockNumber": ("block_number", "long"),
        "timeStamp": ("timestamp", "long"),
        "hash": ("hash", "string"),
        "nonce": ("nonce", "int"),
        "blockHash": ("block_hash", "string"),
        "transactionIndex": ("transaction_index", "int"),
        "from": ("from_address", "string"),
        "to": ("to_address", "string"),
        "value": ("value", "string"),
        "gas": ("gas", "int"),
        "gasPrice": ("gas_price", "long"),
        "isError": ("is_error", "int"),
        "txreceipt_status": ("txreceipt_status", "int"),
        "input": ("input", "string"),
        "contractAddress": ("contract_address", "string"),
        "cumulativeGasUsed": ("cumulative_gas_used", "long"),
        "gasUsed": ("gas_used", "int"),
        "confirmations": ("confirmations", "int"),
        "methodId": ("method_id", "string"),
        "functionName": ("method_name", "string"),
    }

    for key, value in dict_col_names.items():
        df_transactions = df_transactions.withColumnRenamed(key, value[0]).withColumn(value[0], col(value[0]).cast(value[1]))

    df_transactions = df_transactions.withColumn("contract_alias", lit(contract))
    df_transactions = df_transactions.withColumn("dat_ref_carga", lit(date))

    final_cols = [value[0] for key, value in dict_col_names.items()] + ["contract_alias", "dat_ref_carga"]
    df_transactions = df_transactions.select(*final_cols)


    df_transactions.write.mode("append").insertInto(f"{network}.batch_transactions")

