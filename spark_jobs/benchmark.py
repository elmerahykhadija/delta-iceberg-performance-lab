import time
import csv
import os
from pyspark.sql import SparkSession

# Configurations S3 (MinIO)
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET_NAME = "deltavsicberg"

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


def load_delta(spark):
    # Re-lecture à chaque appel : lit toujours le dernier snapshot committé (comme Iceberg).
    return spark.read.format("delta").load(DELTA_TABLE_PATH)


def load_iceberg(spark):
    # Re-lecture à chaque appel : Iceberg bascule automatiquement sur le dernier snapshot.
    return spark.table(ICEBERG_TABLE_NAME)


def run_dml(spark, query):
    """
    Exécute une commande DML (UPDATE / DELETE) EXACTEMENT une fois et renvoie le
    nombre de lignes affectées lisible par le moteur (Delta renvoie num_affected_rows).
    Iceberg (Spark 3.5 + Iceberg 1.5) renvoie un résultat vide : on renvoie alors None
    et l'affichage utilisera le nombre de lignes ciblées précalculé.
    """
    result_df = spark.sql(query)
    try:
        row = result_df.collect()
        if row and len(row[0]) > 0:
            try:
                return int(row[0][0])
            except (TypeError, ValueError):
                return None
    except Exception as exc:
        print(f"[WARN] DML échouée ({query[:80]}...): {exc}")
    return None


def measure_execution(spark, format_name, experiment, operation, description, query_func, run_id, rows_processed):
    spark.catalog.clearCache()  # Comparaison équitable : cache vidé avant chaque mesure

    start_time = time.time()
    result_count = query_func()
    end_time = time.time()

    execution_time_sec = round(end_time - start_time, 4)
    print(f"[{format_name}] {experiment} - {operation} (Run {run_id}): {execution_time_sec} sec | Lignes: {rows_processed}")

    save_result(format_name, experiment, operation, description, run_id, rows_processed, execution_time_sec)


def save_result(format_name, experiment, operation, description, run_id, rows_processed, execution_time_sec):
    file_exists = os.path.isfile(RESULTS_FILE)
    with open(RESULTS_FILE, mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(["format", "experiment", "operation", "description", "run_id", "rows_processed", "execution_time_sec"])
        writer.writerow([format_name, experiment, operation, description, run_id, rows_processed, execution_time_sec])


def run_benchmarks():
    spark = init_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Nombre de lignes de référence (une seule lecture de la source en début de session).
    total_rows = spark.read.parquet("/opt/bitnami/spark/data/source/*.parquet").count()
    formatted_total = f"{total_rows:,}".replace(",", " ")

    UPDATE_VALUES = [99, 100, 101]
    DELETE_VALUES = [4, 5, 6]

    # Réinitialisation propre de la session de benchmark.
    if os.path.isfile(RESULTS_FILE):
        os.remove(RESULTS_FILE)
        print(f"Nouvelle session : '{RESULTS_FILE}' réinitialisé. Source = {formatted_total} lignes.")

    for run_id in range(1, 4):
        new_value = UPDATE_VALUES[run_id - 1]
        delete_value = DELETE_VALUES[run_id - 1]
        update_name = f"Update payment_type → {new_value} (VendorID=2)"
        delete_name = f"Delete payment_type == {delete_value}"

        for format_name, loader in [("Delta", load_delta), ("Iceberg", load_iceberg)]:

            # 1. READ BENCHMARK (Full table scan)
            measure_execution(
                spark, format_name, "READ BENCHMARK", "Full Scan (Count)",
                f"Lecture intégrale de la table ({formatted_total} lignes) sans aucun filtre. "
                f"Cette opération force le scan de tous les fichiers Parquet : elle mesure la vitesse de "
                f"lecture brute et le coût de résolution des métadonnées. Delta liste le dossier via "
                f"_delta_log, Iceberg lit directement ses manifests (pas de lister de répertoire).",
                lambda: loader(spark).count(), run_id, total_rows
            )

            # 2. FILTER BENCHMARK (Sélectivité moyenne)
            measure_execution(
                spark, format_name, "FILTER BENCHMARK", "Filter VendorID = 1",
                f"Filtre de sélectivité moyenne : nous comptons uniquement les lignes dont VendorID = 1 "
                f"(≈ un tiers des lignes). Cette opération teste le 'data skipping' : grâce aux statistiques "
                f"min/max stockées dans les métadonnées, le moteur peut écarter les fichiers ne contenant "
                f"pas la valeur recherchée sans même les décompresser.",
                lambda: loader(spark).filter("VendorID = 1").count(), run_id, None
            )

            # 3. AGGREGATION BENCHMARK
            measure_execution(
                spark, format_name, "AGGREGATION BENCHMARK", "Group by Passenger Count",
                f"Agrégation GROUP BY passenger_count avec SUM(total_amount). Seules 2 colonnes du scan "
                f"colonnaire sont utilisées, puis Spark déclenche un shuffle. Cette opération évalue "
                f"l'aptitude du format à servir des requêtes analytiques complexes (scan + shuffle + reduce).",
                lambda: loader(spark).groupBy("passenger_count").sum("total_amount").count(), run_id, None
            )

            # 4. UPDATE BENCHMARK (Modif payment_type → new_value sur VendorID = 2)
            #    On compte en direct (hors chronomètre) les lignes réellement ciblées, car
            #    les écritures des runs précédents modifient l'état de la table.
            update_target = loader(spark).filter("VendorID = 2").count()

            if format_name == "Delta":
                update_query = f"UPDATE delta.`{DELTA_TABLE_PATH}` SET payment_type = {new_value} WHERE VendorID = 2"
            else:
                update_query = f"UPDATE {ICEBERG_TABLE_NAME} SET payment_type = {new_value} WHERE VendorID = 2"

            measure_execution(
                spark, format_name, "UPDATE BENCHMARK", update_name,
                f"Opération d'écriture ACID : nous avons MODIFIÉ la valeur payment_type → {new_value} "
                f"sur les {update_target:,}".replace(",", " ") + f" lignes dont VendorID = 2 "
                f"(table de {formatted_total} lignes). Cette mise à jour ne réécrit que les fichiers contenant "
                f"les lignes concernées (réécriture de datatiles) puis journalise un commit transactionnel. "
                f"C'est l'opération la plus coûteuse : identique à un MERGE sur un sous-ensemble.",
                lambda q=update_query: run_dml(spark, q), run_id, update_target
            )

            # 5. DELETE BENCHMARK (Suppression payment_type = delete_value)
            #    Comptage en direct APRÈS l'UPDATE : certaines lignes ont pu changer de payment_type.
            delete_target = loader(spark).filter(f"payment_type = {delete_value}").count()

            if format_name == "Delta":
                delete_query = f"DELETE FROM delta.`{DELTA_TABLE_PATH}` WHERE payment_type = {delete_value}"
            else:
                delete_query = f"DELETE FROM {ICEBERG_TABLE_NAME} WHERE payment_type = {delete_value}"

            measure_execution(
                spark, format_name, "DELETE BENCHMARK", delete_name,
                f"Opération d'écriture ACID : nous avons SUPPRIMÉ toutes les lignes dont "
                f"payment_type = {delete_value} ({delete_target:,}".replace(",", " ")
                + f" lignes ciblées après l'UPDATE). La suppression réécrit les fichiers concernés sans "
                  f"les lignes cibles puis journalise un commit transactionnel.",
                lambda q=delete_query: run_dml(spark, q), run_id, delete_target
            )

    spark.stop()
    print(f"\n✅ Benchmark terminé. Résultats sauvegardés dans {RESULTS_FILE}")


if __name__ == "__main__":
    run_benchmarks()