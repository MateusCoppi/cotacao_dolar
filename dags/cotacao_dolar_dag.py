from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
from airflow.utils.dates import days_ago
import os 

with DAG ('cotacao_dolar_full', schedule_interval='@daily', start_date=days_ago(1)) as dag_trigger:
    trigger_task = TriggerDagRunOperator(
        task_id='trigger_cotacao_dolar',
        trigger_dag_id='transformacao_dolar_silver',
        reset_dag_run=True
    )