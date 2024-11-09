from pyspark.sql import SparkSession

def main():
    spark = SparkSession \
        .builder \
        .appName('teste_airflow') \
        .getOrCreate()
    
    # spark.stop()
