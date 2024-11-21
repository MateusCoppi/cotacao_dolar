from airflow.operators.empty import EmptyOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator 
from airflow import DAG
from airflow.utils.dates import days_ago
import os
import sys

sys.path.append(os.path.abspath('/opt/airflow/scripts/transformacoes/cotacao_dolar'))

from cotacao_dolar_silver import main

with DAG(
    dag_id='dolar_data_silver',
    start_date=days_ago(1),
    schedule="@daily"
) as transformation_dag:
    
    start = EmptyOperator(task_id='start_transformation')
    
    transformacao = PythonOperator(
        task_id='dolar_silver_transformation',
        python_callable=main
    )

    end = EmptyOperator(task_id='end_transformation')

    start >> transformacao >> end