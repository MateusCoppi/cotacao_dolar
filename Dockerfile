FROM apache/airflow:2.10.2-python3.12
COPY requirements.txt /

USER root

RUN apt-get update && apt-get install -y openjdk-17-jdk
RUN chown -R airflow: /opt/airflow/logs

USER airflow

ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV SPARK_HOME="/opt/airflow/spark/spark-3.5.3-bin-hadoop3"
ENV PATH="$JAVA_HOME/bin:$SPARK_HOME/bin:${PATH}"
ENV PYTHONPATH="/opt/airflow"

RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install apache-airflow apache-airflow-providers-apache-spark pyspark