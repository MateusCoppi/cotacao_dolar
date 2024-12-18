from minio import Minio
from minio.error import S3Error
import io
import json
from pyspark.sql import SparkSession
import os

class MinionConnection:

    def __init__(self, access_key, secret_key, host_name):
        self.access_key = access_key
        self.secret_key = secret_key
        self.host_name = host_name

    # Conexao com servidor Minio (container)
    def conexao_minio(self):
        # print("Iniciando Conexão com o MinIO")
        client = Minio(
            self.host_name, # "minio:9000", MinIO e porta *docker*     
            access_key=self.access_key,  # Chave de acesso do MinIO
            secret_key=self.secret_key,  # Chave secreta do MinIO
            secure=False  # Usar HTTP (False) ou HTTPS (True)
            )
        # print("Conexão com o MinIO realizada com sucesso")
        return client

    def lista_buckets(self):
        client = self.conexao_minio()
        try:
            buckets = client.list_buckets()
            print(f"Buckets Encontrados: {buckets}")
        except S3Error as err:
            print(f"Erro ao conectar ao MinIO: {err}")

    def import_json_to_bucket(self, object_name, bucket_name, file):
        client = self.conexao_minio()
        arquivo_io = io.BytesIO(json.dumps(file).encode('utf-8'))
        try:
            client.put_object(
                bucket_name=bucket_name,
                object_name=object_name, # Object_name -> nome que será utilizado no arquivo dentro do bucket.
                data=arquivo_io,
                length=len(arquivo_io.getvalue()),
                content_type='application/json'
            )
            print(f"Arquivo '{object_name}' enviado com sucesso para o bucket '{bucket_name}'!")
        except S3Error as e:
            print(f"Erro ao fazer upload do arquivo: {e}")

    def get_json_objects_from_bucket(self, bucket: str):
        client = self.conexao_minio()
        files = client.list_objects(bucket, recursive=True)
        json_data = []
        try:            
            for obj in files:
                response = self.conexao_minio().get_object(bucket, obj.object_name)
                try:
                    file_data = response.read().decode('utf-8')
                    json_data.append(json.loads(file_data))
                finally:
                    response.close()
                    response.release_conn()
        except S3Error as e:
            print(f"Erro ao obter os arquivos {e}") 
        
        return json_data
    
    # def get_csv_objects(self, bucket: str, object: str):
    #     os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-1.17.0-openjdk-amd64"
    #     os.environ["SPARK_HOME"] = "/opt/spark/spark-3.5.3-bin-hadoop3"

    #     spark = SparkSession \
    #         .builder \
    #         .appName("csv_minio") \
    #         .master("local[4]") \
    #         .config("spark.driver.memory", "4g") \
    #         .config("spark.executor.memory", "4g") \
    #         .config("fs.s3a.access.key", "mateus1234") \
    #         .config("fs.s3a.secret.key", "cofre1234") \
    #         .config("fs.s3a.endpoint", "http://localhost:9000") \
    #         .config("fs.s3a.path.style.access", "true") \
    #         .config("fs.s3a.connection.ssl.enabled", "false") \
    #         .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.630') \
    #         .getOrCreate()
        
    #     s3_path = f"s3a://{bucket}/{object}"        
    #     try:
    #         df = spark.read.csv(s3_path, header=True, inferSchema=True)
    #         df.show(5)
    #         return df
    #     except Exception as e:
    #         print("Erro ao ler o CSV do MinIO:", e)
    #     raise