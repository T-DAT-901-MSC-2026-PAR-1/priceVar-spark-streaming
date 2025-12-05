import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    expr,
    from_json,
    lit,
    to_json,
    struct,
    window,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


SPARK_APP_NAME: str = os.environ.get("SPARK_APP_NAME")
SPARK_MASTER_URL: str = os.environ.get("SPARK_MASTER_URL")
KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_SUBSCRIBE_TOPICS: str = os.environ.get("KAFKA_SUBSCRIBE_TOPICS")

# Output topic (single topic). Consumers can filter by key or fields in the payload.
KAFKA_OUTPUT_TOPIC: str = os.environ.get("KAFKA_OUTPUT_TOPIC", "pricevar-results")

# Windowing parameters (defaults match previous behavior)
WINDOW_DURATION: str = os.environ.get("WINDOW_DURATION", "1 minute")
WINDOW_SLIDE: str = os.environ.get("WINDOW_SLIDE", "10 seconds")
WATERMARK_DELAY: str = os.environ.get("WATERMARK_DELAY", "30 seconds")


trade_schema = StructType(
    [
        StructField("type", StringType(), True),
        StructField("market", StringType(), True),
        StructField("from_symbol", StringType(), True),
        StructField("to_symbol", StringType(), True),
        StructField("flags", StringType(), True),
        StructField("trade_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("price", StringType(), True),
        StructField("total_value", StringType(), True),
        StructField("received_ts", StringType(), True),
        StructField("ccseq", StringType(), True),
        StructField("timestamp_ns", StringType(), True),
        StructField("received_ts_ns", StringType(), True),
    ]
)

spark: SparkSession = (
    SparkSession.builder.master(SPARK_MASTER_URL)
    .appName(SPARK_APP_NAME)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    # Align with entrypoint.sh runtime (Scala 2.13 / Spark 4.0.1)
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
    .config("spark.sql.shuffle.partitions", "20")
    .config("spark.streaming.kafka.maxRatePerPartition", "50")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

raw_trades = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribePattern", KAFKA_SUBSCRIBE_TOPICS)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("kafka.request.timeout.ms", "120000")
    .option("kafka.session.timeout.ms", "90000")
    .option("kafka.default.api.timeout.ms", "120000")
    .option("kafka.connections.max.idle.ms", "180000")
    .option("maxOffsetsPerTrigger", "10000")
    .load()
)

parsed_trades = (
    raw_trades.select(from_json(col("value").cast("string"), trade_schema).alias("data"))
    .select("data.*")
    # Event-time in seconds since epoch → Spark timestamp
    .withColumn("timestamp_dt", (col("timestamp").cast("long")).cast("timestamp"))
    .withColumn("price_num", col("price").cast(DoubleType()))
    .filter(col("price_num").isNotNull() & col("timestamp_dt").isNotNull())
    .withWatermark("timestamp_dt", WATERMARK_DELAY)
)

# Per window:
# - open_price = first price by event time
# - close_price = last price by event time
# - abs_change = close - open
# - pct_change = abs_change / open * 100
price_variation = (
    parsed_trades.groupBy(
        window(col("timestamp_dt"), WINDOW_DURATION, WINDOW_SLIDE),
        col("market"),
        col("from_symbol"),
        col("to_symbol"),
    )
    .agg(
        expr("min_by(price_num, timestamp_dt)").alias("open_price"),
        expr("max_by(price_num, timestamp_dt)").alias("close_price"),
    )
    .withColumn("abs_change", col("close_price") - col("open_price"))
    .withColumn(
        "pct_change",
        expr(
            "CASE WHEN open_price IS NULL OR open_price = 0 THEN NULL "
            "ELSE (close_price - open_price) / open_price * 100 END"
        ),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("market"),
        col("from_symbol"),
        col("to_symbol"),
        col("close_price").alias("last_price"),
        col("abs_change"),
        col("pct_change"),
    )
)


def main():
    output_dataframe = (
        price_variation.withColumn("key", concat(col("from_symbol"), lit("_"), col("to_symbol")))
        .withColumn("topic", lit(KAFKA_OUTPUT_TOPIC))
        .withColumn(
            "value",
            to_json(
                struct(
                    col("window_start"),
                    col("window_end"),
                    col("market"),
                    col("from_symbol"),
                    col("to_symbol"),
                    col("last_price"),
                    col("abs_change"),
                    col("pct_change"),
                )
            ),
        )
        .selectExpr(
            "CAST(topic AS STRING) as topic",
            "CAST(key AS STRING) as key",
            "CAST(value AS STRING) as value",
        )
    )

    # Debug (optional)
    # query = (
    #     output_dataframe.writeStream.outputMode("update")
    #     .format("console")
    #     .option("truncate", "false")
    #     .start()
    # )
    # query.awaitTermination()

    queryKafka = (
        output_dataframe.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("checkpointLocation", "/tmp/pricevar-checkpoint")
        .start()
    )

    queryKafka.awaitTermination()


if __name__ == "__main__":
    main()
