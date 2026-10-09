import boto3
import pandas as pd
from botocore.client import Config

# Configurations MinIO
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET = "deltavsicberg"

def get_folder_size(s3_client, prefix):
    total_size = 0
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                total_size += obj['Size']
    return total_size / (1024 * 1024) # Retourne la taille en MB

def main():
    s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, aws_access_key_id=MINIO_ACCESS_KEY, 
                      aws_secret_access_key=MINIO_SECRET_KEY, config=Config(signature_version='s3v4'))

    # Taille Delta
    delta_data = get_folder_size(s3, "delta/yellow_tripdata/")
    delta_meta = get_folder_size(s3, "delta/yellow_tripdata/_delta_log/")
    delta_pure_data = delta_data - delta_meta

    # Taille Iceberg (Les données sont dans /data/ et métadonnées dans /metadata/)
    iceberg_data = get_folder_size(s3, "iceberg/default/yellow_tripdata/data/")
    iceberg_meta = get_folder_size(s3, "iceberg/default/yellow_tripdata/metadata/")

    results = [
        {"Format": "Delta", "Type": "Data (MB)", "Size": delta_pure_data},
        {"Format": "Delta", "Type": "Metadata (MB)", "Size": delta_meta},
        {"Format": "Iceberg", "Type": "Data (MB)", "Size": iceberg_data},
        {"Format": "Iceberg", "Type": "Metadata (MB)", "Size": iceberg_meta}
    ]
    
    df = pd.DataFrame(results)
    df.to_csv("/opt/bitnami/spark/data/storage_metrics.csv", index=False)
    print("Métrique de stockage exportée avec succès.")

if __name__ == "__main__":
    main()
