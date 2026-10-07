import os
import boto3
from botocore.client import Config
from pyspark.sql import SparkSession

# Configurations S3 (MinIO) - Voir docker-compose.yml
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET_NAME = "deltavsicberg"

def create_bucket_if_not_exists():
    """Crée le bucket dans MinIO s'il n'existe pas déjà."""
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
        print(f"Le bucket '{BUCKET_NAME}' existe déjà.")
    except Exception:
        print(f"Le bucket '{BUCKET_NAME}' n'existe pas, création en cours...")
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' créé avec succès.")

def main():
    # 1. Vérification/Création du bucket S3 dans MinIO
    create_bucket_if_not_exists()
    
    # 2. Initialisation de la SparkSession avec les configurations pour Delta, Iceberg et S3
    print("Initialisation de la SparkSession...")
    spark = SparkSession.builder \
        .appName("Ingestion Delta and Iceberg") \
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

    # Niveau de log pour réduire le bruit
    spark.sparkContext.setLogLevel("WARN")

    # 3. Lecture des données sources (fichiers Parquet montés dans le container)
    source_path = "/opt/bitnami/spark/data/source/*.parquet"
    print(f"Lecture des données depuis : {source_path}")
    
    df = spark.read.parquet(source_path)
    print(f"Nombre de lignes lues : {df.count()}")
    df.printSchema()

    # ==========================================
    # Approche 1 : Sauvegarde au format Delta
    # ==========================================
    delta_path = f"s3a://{BUCKET_NAME}/delta/yellow_tripdata"
    print(f"Début de la sauvegarde en format DELTA vers {delta_path}...")
    
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(delta_path)
        
    print("✅ Sauvegarde Delta terminée avec succès.")

    # ==========================================
    # Approche 2 : Sauvegarde au format Iceberg
    # ==========================================
    # En utilisant le catalog 'iceberg_catalog' configuré plus haut
    iceberg_table = "iceberg_catalog.default.yellow_tripdata"
    print(f"Début de la sauvegarde en format ICEBERG dans la table {iceberg_table}...")
    
    df.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(iceberg_table)
        
    print("✅ Sauvegarde Iceberg terminée avec succès.")

    # Arrêt de la session Spark
    spark.stop()
    print("Processus d'ingestion terminé.")

if __name__ == "__main__":
    main()
