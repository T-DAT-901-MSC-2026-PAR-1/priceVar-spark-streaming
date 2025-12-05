#!/usr/bin/env bash
set -euo pipefail

printf "[entrypoint] starting spark submit runner\n"

# Required environment variables (no defaults - fail fast if missing)
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"
: "${KAFKA_SUBSCRIBE_TOPICS:?KAFKA_SUBSCRIBE_TOPICS is required}"
: "${KAFKA_OUTPUT_TOPIC:?KAFKA_OUTPUT_TOPIC is required}"
: "${SPARK_APP_NAME:?SPARK_APP_NAME is required}"
: "${SPARK_MASTER_URL:?SPARK_MASTER_URL is required}"

# Allow passing an alternative command to the container (for debugging)
if [ "$#" -gt 0 ]; then
  echo "[entrypoint] executing provided command: $@"
  exec "$@"
fi

# Construct spark-submit command
SPARK_CMD=(/opt/spark/bin/spark-submit
  --master "${SPARK_MASTER_URL}"
  --deploy-mode client
  --conf "spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
  --conf "spark.driver.host=priceVar-spark-submit-runner"
  --conf "spark.driver.bindAddress=0.0.0.0"
  --conf "spark.executorEnv.KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS}"
  --conf "spark.executorEnv.KAFKA_SUBSCRIBE_TOPICS=${KAFKA_SUBSCRIBE_TOPICS}"
  --conf "spark.executorEnv.KAFKA_OUTPUT_TOPIC=${KAFKA_OUTPUT_TOPIC}"
  --conf "spark.executorEnv.SPARK_APP_NAME=${SPARK_APP_NAME}"
  --conf "spark.executorEnv.SPARK_MASTER_URL=${SPARK_MASTER_URL}"
  /opt/app/main.py
)

echo "[entrypoint] running: ${SPARK_CMD[*]}"
exec "${SPARK_CMD[@]}"
