from minio import Minio
from minio.error import S3Error
import io
import json

class MinionConnection:

    def __init__(self, access_key, secret_key):
        self.access_key = access_key
        self.secret_key = secret_key
    

    # Conexao com servidor Minio (container)
    def conexao_minio(self):
        print("Iniciando Conexão com o MinIO")
        client = Minio(
            "minio:9000",  # MinIO e porta
            access_key=self.access_key,  # Chave de acesso do MinIO
            secret_key=self.secret_key,  # Chave secreta do MinIO
            secure=False  # Usar HTTP (False) ou HTTPS (True)
            )
        print("Conexão com o MinIO realizada com sucesso")
        return client

    def lista_buckets(self):
        client = self.conexao_minio()
        try:
            buckets = client.list_buckets()
            print(f"Buckets Encontrados: {buckets}")
        except S3Error as err:
            print(f"Erro ao conectar ao MinIO: {err}")

    def import_data_to_bucket(self, object_name, bucket_name, file):
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