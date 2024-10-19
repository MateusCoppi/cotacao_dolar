import sys
import os
import json
import findspark
from pyspark.sql import SparkSession
import ast
import json

# Caminho para importacao da classe do Minio
sys.path.append(os.path.abspath('/home/mateus_coppi/Documents/apache-airflow/scripts'))
from minio_connection import MinionConnection

# Variavel necessaria para funcionamento do PySpark
os.environ["SPARK_HOME"] = "/opt/spark/spark-3.5.3-bin-hadoop3"

minio_connect = MinionConnection(access_key="myminioadmin", secret_key="minio-secret-key-change-me", host_name="localhost:9000")
minio_client = minio_connect.conexao_minio()

# Pegando os dados das cotacoes do bucket_bronze
data_bucket_json = minio_connect.get_json_objects_from_bucket(bucket='dolar-bucket-bronze')

findspark.init()

spark = SparkSession \
    .builder \
    .appName("transformacao_cotacao_dolar") \
    .getOrCreate()


obj_dict = []
data_dict = []
# Transforma uma str em formato de dicionario para dicionario
# Pega somente os valores dos dados da cotacao (cotacao e datas)
def str_to_dict(json_str):
    for dict_str in json_str:    
        dict_obj = (ast.literal_eval(dict_str))
        obj_dict.append(dict_obj)
        for i in obj_dict:
            data_dict.append(i['value'])
    return data_dict

dados_formatados = []
# Formata os dados em uma unica lista de dicionarios
def dicts_to_list(dict):
    for list in dict:
        for row in list:
            dados_formatados.append(row)
    return dados_formatados

if __name__ == "__main__":

    dados_formatados = dicts_to_list(str_to_dict(data_bucket_json))
    df = spark.createDataFrame(dados_formatados)
    df.show(25)