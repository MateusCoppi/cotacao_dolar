import sys
import os
from pyspark.sql import SparkSession
import ast

# Caminho para importacao da classe do Minio
sys.path.append(os.path.abspath('/opt/airflow/scripts')) # path container
# sys.path.append(os.path.abspath('/home/mateus_coppi/Documents/apache-airflow/scripts')) ## path local
from minio_connection import MinionConnection

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-1.17.0-openjdk-amd64" # path java container
os.environ["SPARK_HOME"] = "/opt/airflow/spark/spark-3.5.3-bin-hadoop3" # path container

# Transforma uma str em formato de dicionario para dicionario
# Pega somente os valores dos dados da cotacao (cotacao e datas)
def str_to_dict(json_str):
    obj_dict = []
    data_dict = []    
    for dict_str in json_str:    
        dict_obj = (ast.literal_eval(dict_str))
        obj_dict.append(dict_obj)
        for i in obj_dict:
            data_dict.append(i['value'])
    return data_dict

# Formata os dados em uma unica lista de dicionarios
def dicts_to_list(dict):
    dados_formatados = []
    for list in dict:
        for row in list:
            dados_formatados.append(row)
    return dados_formatados

def main():

    minio_connect = MinionConnection(access_key="myminioadmin", secret_key="minio-secret-key-change-me", host_name="minio:9000")
    minio_client = minio_connect.conexao_minio()

    # Pegando os dados das cotacoes do bucket_bronze
    data_bucket_json = minio_connect.get_json_objects_from_bucket(bucket='dolar-bucket-bronze')

    # findspark.init()

    spark = SparkSession \
        .builder \
        .appName("spark-cotacao-dolar-siver-minio") \
        .master("spark://spark:7077") \
        .config("fs.s3a.access.key", "myminioadmin") \
        .config("fs.s3a.secret.key", "minio-secret-key-change-me") \
        .config("fs.s3a.endpoint", "http://minio:9000") \
        .config("fs.s3a.path.style.access", "true") \
        .config("fs.s3a.connection.ssl.enabled", "false") \
        .config('spark.jars.packages','org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-common:3.3.4') \
        .getOrCreate()
    

    bucket = "s3a://silver/cotacao_dolar_parquet"
    dados_formatados = dicts_to_list(str_to_dict(data_bucket_json))
    df = spark.createDataFrame(dados_formatados)
    df.write.format('parquet').mode('overwrite').save(f'{bucket}')
    print(df.show(5))

if __name__ == "__main__":
    main()