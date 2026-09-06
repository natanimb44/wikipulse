"""
Spark Structured Streaming job: consumes `wiki-edits` from Redpanda, computes
1-minute tumbling-window aggregates per page and per editor, maintains an EWMA
baseline + variance per entity (state stored in TimescaleDB and read/updated
each micro-batch via foreachBatch — the "simplest v1" approach called out in
the project brief), flags anomalies, and writes results to TimescaleDB.
"""
import math
import os

import psycopg2
import psycopg2.extras
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    from_unixtime,
    lit,
    lower,
    sum as ssum,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import BooleanType, IntegerType, LongType, StringType, StructField, StructType

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "redpanda:9092")
TOPIC = os.environ.get("WIKI_EDITS_TOPIC", "wiki-edits")

PG_HOST = os.environ.get("TIMESCALE_HOST", "timescaledb")
PG_PORT = os.environ.get("TIMESCALE_PORT", "5432")
PG_DB = os.environ.get("TIMESCALE_DB", "wikipulse")
PG_USER = os.environ.get("TIMESCALE_USER", "wikipulse")
PG_PASSWORD = os.environ.get("TIMESCALE_PASSWORD", "wikipulse")

EWMA_ALPHA = float(os.environ.get("EWMA_ALPHA", "0.3"))
Z_THRESHOLD = float(os.environ.get("Z_THRESHOLD", "3.0"))
REVERT_RATIO_THRESHOLD = float(os.environ.get("REVERT_RATIO_THRESHOLD", "0.5"))
MIN_STDDEV = 1.0  # floor to avoid divide-by-near-zero blowing up z-scores early on

EVENT_SCHEMA = StructType([
    StructField("title", StringType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),
    StructField("anon", BooleanType()),
    StructField("timestamp", LongType()),
    StructField("type", StringType()),
    StructField("wiki", StringType()),
    StructField("namespace", IntegerType()),
    StructField("length_old", LongType()),
    StructField("length_new", LongType()),
    StructField("revision_old", LongType()),
    StructField("revision_new", LongType()),
    StructField("comment", StringType()),
    StructField("server_url", StringType()),
])


def pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
    )


def severity_for(z: float) -> str:
    az = abs(z)
    if az >= 6:
        return "high"
    if az >= 4:
        return "medium"
    return "low"


def process_batch(batch_df: DataFrame, epoch_id: int) -> None:
    rows = batch_df.collect()
    if not rows:
        return

    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            for row in rows:
                entity_type = row["entity_type"]
                entity_key = row["entity_key"]
                edit_count = row["edit_count"]
                revert_count = row["revert_count"] or 0
                anon_ratio = row["anon_ratio"]

                cur.execute(
                    "SELECT ewma, variance FROM entity_baseline WHERE entity_type = %s AND entity_key = %s",
                    (entity_type, entity_key),
                )
                existing = cur.fetchone()
                if existing is None:
                    baseline_ewma, baseline_var = float(edit_count), 0.0
                else:
                    baseline_ewma, baseline_var = existing

                stddev = max(math.sqrt(baseline_var), MIN_STDDEV)
                z_score = (edit_count - baseline_ewma) / stddev

                new_ewma = EWMA_ALPHA * edit_count + (1 - EWMA_ALPHA) * baseline_ewma
                new_var = (
                    EWMA_ALPHA * (edit_count - baseline_ewma) ** 2
                    + (1 - EWMA_ALPHA) * baseline_var
                )

                cur.execute(
                    """
                    INSERT INTO window_stats
                        (window_start, window_end, entity_type, entity_key,
                         edit_count, revert_count, anon_ratio, baseline_ewma, baseline_var, z_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (window_start, entity_type, entity_key) DO UPDATE SET
                        edit_count = EXCLUDED.edit_count,
                        revert_count = EXCLUDED.revert_count,
                        anon_ratio = EXCLUDED.anon_ratio,
                        baseline_ewma = EXCLUDED.baseline_ewma,
                        baseline_var = EXCLUDED.baseline_var,
                        z_score = EXCLUDED.z_score
                    """,
                    (
                        row["window_start"], row["window_end"], entity_type, entity_key,
                        edit_count, revert_count, anon_ratio, baseline_ewma, baseline_var, z_score,
                    ),
                )

                cur.execute(
                    """
                    INSERT INTO entity_baseline (entity_type, entity_key, ewma, variance, updated_at)
                    VALUES (%s, %s, %s, %s, now())
                    ON CONFLICT (entity_type, entity_key) DO UPDATE SET
                        ewma = EXCLUDED.ewma, variance = EXCLUDED.variance, updated_at = now()
                    """,
                    (entity_type, entity_key, new_ewma, new_var),
                )

                if abs(z_score) > Z_THRESHOLD:
                    cur.execute(
                        """
                        INSERT INTO anomalies
                            (window_start, entity_type, entity_key, metric, value, baseline, z_score, severity)
                        VALUES (%s, %s, %s, 'edit_rate', %s, %s, %s, %s)
                        """,
                        (
                            row["window_start"], entity_type, entity_key,
                            float(edit_count), baseline_ewma, z_score, severity_for(z_score),
                        ),
                    )

                if entity_type == "page" and edit_count > 0:
                    revert_ratio = revert_count / edit_count
                    if revert_ratio > REVERT_RATIO_THRESHOLD:
                        pseudo_z = revert_ratio / REVERT_RATIO_THRESHOLD
                        cur.execute(
                            """
                            INSERT INTO anomalies
                                (window_start, entity_type, entity_key, metric, value, baseline, z_score, severity)
                            VALUES (%s, %s, %s, 'revert_rate', %s, %s, %s, %s)
                            """,
                            (
                                row["window_start"], entity_type, entity_key,
                                revert_ratio, REVERT_RATIO_THRESHOLD, pseudo_z, severity_for(pseudo_z),
                            ),
                        )
        conn.commit()
    finally:
        conn.close()


def build_entity_aggregate(parsed: DataFrame, key_col: str, entity_type: str) -> DataFrame:
    return (
        parsed
        .filter(col(key_col).isNotNull())
        .withWatermark("event_time", "2 minutes")
        .groupBy(window(col("event_time"), "1 minute"), col(key_col).alias("entity_key"))
        .agg(
            count("*").alias("edit_count"),
            ssum(when(col("is_revert"), 1).otherwise(0)).alias("revert_count"),
            avg(when(col("anon"), 1.0).otherwise(0.0)).alias("anon_ratio"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            lit(entity_type).alias("entity_type"),
            col("entity_key"),
            col("edit_count"),
            col("revert_count"),
            col("anon_ratio"),
        )
    )


def main():
    spark = SparkSession.builder.appName("wikipulse-stream-processor").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp"))))
        .withColumn(
            "is_revert",
            lower(col("comment")).rlike("revert|undo"),
        )
    )

    page_agg = build_entity_aggregate(parsed, "title", "page")
    editor_agg = build_entity_aggregate(parsed, "user", "editor")

    page_query = (
        page_agg.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", "/tmp/checkpoints/page")
        .start()
    )

    editor_query = (
        editor_agg.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", "/tmp/checkpoints/editor")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
