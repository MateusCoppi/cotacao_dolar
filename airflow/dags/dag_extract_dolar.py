from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from airflow import DAG
from time import sleep
import os
import sys

sys.path.append(os.path.abspath('/opt/airflow/scripts')) # path dos scripts
sys.path.append(os.path.abspath('/opt/airflow/scripts/transformacoes/cotacao_dolar')) 
from minio_connection import MinionConnection
from extracao_cotacao_dolar import extracao_cotacao_dolar_bacen
import cotacao_dolar_silver


# importa os dados json para o bucket_bronze
def to_bronze(arquivo, data_interval_start, **kwargs):
    data = pendulum.parse(data_interval_start).strftime("%m-%d-%Y")
    minio_connect = MinionConnection(access_key="user", secret_key="password", host_name="minio:9000")
    minio_connect.lista_buckets()
    minio_connect.import_json_to_bucket(object_name=f"cotacao{data}", bucket_name="dolar-bucket-bronze", file=arquivo)
    sleep(5)


with DAG(
    dag_id="cotacao_dolar_to_minio",
    start_date=pendulum.datetime(2024, 10, 20, tz="UTC"),
    schedule="@daily",
    catchup=True
) as dag_extracao:
    
    start_extract = EmptyOperator(task_id="start_extract")

    extract_operator = PythonOperator(
        task_id="extract_dolar_context",
        python_callable=extracao_cotacao_dolar_bacen,
        op_kwargs={"data_interval_start": "{{ data_interval_start }}"}
    )

    end_extract = EmptyOperator(task_id="end_extract")

    load_operator = PythonOperator(
        task_id='load_dolar',
        python_callable=to_bronze,
        op_kwargs={'arquivo': "{{ ti.xcom_pull(task_ids='extract_dolar_context') }}", "data_interval_start": "{{ data_interval_start }}"} 
    )

    end_load = EmptyOperator(task_id='end_load')

    # start_transformation = EmptyOperator(task_id='start_transformacao')
    
    # trigger_transformacao_silver = TriggerDagRunOperator(
    #     task_id='trigger_silver',
    #     trigger_dag_id='transformation_dag',
    #     dag=dag_extracao
    # )

    # end_transformation = EmptyOperator(task_id='end_transformation')


    start_extract >> extract_operator >> end_extract >> load_operator >> end_load
