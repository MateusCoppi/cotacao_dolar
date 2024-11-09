from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG
from airflow.utils.dates import days_ago
import os
import sys

sys.path.append(os.path.abspath('/opt/airflow/scripts/transformacoes/cotacao_dolar'))

with DAG(
    dag_id='transformacao_dolar_silver',
    start_date=days_ago(1),
    default_args={
        "owner": "Mateus Copi"
    },
    schedule="@daily"
) as dag:
    
    start = EmptyOperator(task_id='start_transformation')
    
    transformacao = SparkSubmitOperator(
        task_id='silver_cotacao_dolar_spark',
        application="/opt/airflow/scripts/transformacoes/cotacao_dolar/cotacao_dolar_silver.py",
        conn_id="spark_default",
        verbose=False
    )

    end = EmptyOperator(task_id='end_transformation')

    start >> transformacao >> end