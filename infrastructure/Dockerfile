FROM bitnamilegacy/spark:3.5.0

USER root

# Installation des outils de base
RUN apt-get update && \
    apt-get install -y python3-pip curl && \
    rm -rf /var/lib/apt/lists/*

# Installation des dépendances Python
COPY requirements.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Téléchargement des JARs pour MinIO (S3), Delta Lake et Apache Iceberg
RUN cd /opt/bitnami/spark/jars/ && \
    # Connecteurs S3 / MinIO
    curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    # Bibliothèques Delta Lake 3.1.0
    curl -O https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \
    curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar && \
    # Bibliothèque Apache Iceberg 1.5.0
    curl -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar

# Retour à l'utilisateur non-root
USER 1001