import time
import csv
import os
from pyspark.sql import SparkSession

# Configurations S3 (MinIO)
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET_NAME = "deltavsicberg"

# Configuration des chemins
DELTA_TABLE_PATH = f"s3a://{BUCKET_NAME}/delta/yellow_tripdata"
ICEBERG_TABLE_NAME = "iceberg_catalog.default.yellow_tripdata"
RESULTS_FILE = "/opt/bitnami/spark/data/benchmark_results.csv"

def init_spark():
    return SparkSession.builder \
        .appName("Lakehouse-Benchmark") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
        .config("spark.sql.catalog.iceberg_catalog.warehouse", f"s3a://{BUCKET_NAME}/iceberg/") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def measure_execution(spark, format_name, experiment, operation, query_func, run_id):
    """
    Exécute une fonction Spark, mesure son temps d'exécution et vide le cache.
    """
    # Nettoyage du cache pour une comparaison équitable
    spark.catalog.clearCache()
    
    start_time = time.time()
    
    # Exécution de la requête (l'action de type collect(), count() ou show() doit être dans query_func)
    result_count = query_func()
    
    end_time = time.time()
    execution_time_sec = round(end_time - start_time, 4)
    
    print(f"[{format_name}] {experiment} - {operation} (Run {run_id}): {execution_time_sec} sec (Lignes: {result_count})")
    
    # Sauvegarde des résultats
    save_result(format_name, experiment, operation, run_id, execution_time_sec)

def save_result(format_name, experiment, operation, run_id, execution_time_sec):
    file_exists = os.path.isfile(RESULTS_FILE)
    
    with open(RESULTS_FILE, mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            # Création de l'en-tête dictée par tes spécifications
            writer.writerow(["format", "experiment", "operation", "run_id", "execution_time_sec"])
        
        writer.writerow([format_name, experiment, operation, run_id, execution_time_sec])

def run_benchmarks():
    spark = init_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Chargement des DataFrames (Lazy evaluation : aucune donnée n'est lue ici)
    df_delta = spark.read.format("delta").load(DELTA_TABLE_PATH)
    df_iceberg = spark.table(ICEBERG_TABLE_NAME)

    formats = {
        "Delta": df_delta,
        "Iceberg": df_iceberg
    }

    NUM_RUNS = 3 # Exécuter plusieurs fois pour calculer la moyenne plus tard

    for format_name, df in formats.items():
        for run_id in range(1, NUM_RUNS + 1):
            
            # 1. READ BENCHMARK (Full table scan)
            measure_execution(
                spark, format_name, "READ BENCHMARK", "Full Scan (Count)", 
                lambda: df.count(), 
                run_id
            )

            # 2. FILTER BENCHMARK (Sélectivité moyenne)
            # Adapté aux données NYC Taxi (remplace 'VendorID' par une colonne de ton Parquet si nécessaire)
            measure_execution(
                spark, format_name, "FILTER BENCHMARK", "Filter VendorID = 1", 
                lambda: df.filter("VendorID = 1").count(), 
                run_id
            )

            # 3. AGGREGATION BENCHMARK
            measure_execution(
                spark, format_name, "AGGREGATION BENCHMARK", "Group by Passenger Count", 
                lambda: df.groupBy("passenger_count").sum("total_amount").count(), 
                run_id
            )

    spark.stop()

if __name__ == "__main__":
    run_benchmarks()