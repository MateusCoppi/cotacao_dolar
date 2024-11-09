from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG
from airflow.utils.dates import days_ago
import os
import sys

sys.path.append(os.path.abspath('/opt/airflow/scripts'))

with DAG(
    dag_id='teste_spark_dag',
    start_date=days_ago(1),
    schedule="@daily"
) as dag:

    teste_spark = SparkSubmitOperator(
        task_id='teste_spark',
        application="/opt/airflow/scripts/teste_spark.py",
        conn_id="spark_default"
    )

    end = EmptyOperator(task_id='end')



teste_spark >> end