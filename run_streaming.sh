set -e

echo "Starting services..."
docker compose up -d --build

echo "Waiting for services to be ready..."
sleep 30

echo "Kafka topics:"
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092

echo "Starting Spark Structured Streaming job..."
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  /opt/spark-streaming/streaming_job.py
