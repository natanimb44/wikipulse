#!/bin/bash
set -e

# Cost-conscious deployment: the producer (SSE -> Kafka) and the Spark
# streaming job share a single Railway service/container here, since the
# Trial plan caps the project at 4 services. Producer runs in the
# background; Spark runs in the foreground so the container's lifecycle
# tracks the more important of the two processes.
python3 /app/producer.py &
PRODUCER_PID=$!

cleanup() {
  kill "$PRODUCER_PID" 2>/dev/null || true
}
trap cleanup EXIT

exec /opt/spark/bin/spark-submit \
  --master local[1] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.kafka:kafka-clients:3.5.1 \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.driver.memory=512m \
  --conf spark.ui.enabled=false \
  --conf spark.driver.extraJavaOptions="-XX:MaxMetaspaceSize=256m -XX:MaxDirectMemorySize=64m -XX:+UseSerialGC" \
  /app/stream_processor.py
