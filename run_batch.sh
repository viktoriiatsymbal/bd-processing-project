#!/usr/bin/env bash
set -euo pipefail

echo "Starting Spark batch job..."

docker compose exec -e HOME=/tmp spark-master /bin/sh -lc '
  mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars

  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=wikiuser \
    --conf spark.hadoop.fs.s3a.secret.key=wikipass \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    --conf spark.cassandra.connection.host=cassandra \
    /opt/spark-batch/job.py
'
