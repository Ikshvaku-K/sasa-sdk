"""
SASA Spark Structured Streaming job.
Reads all ingested events and produces per-project aggregations.

Run:  python spark/streaming_job.py
"""
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

BASE = Path(__file__).parent

SCHEMA = StructType([
    StructField("event_id",    StringType()),
    StructField("project_id",  StringType()),
    StructField("session_id",  StringType()),
    StructField("user_id",     StringType()),
    StructField("event_name",  StringType()),
    StructField("url",         StringType()),
    StructField("path",        StringType()),
    StructField("title",       StringType()),
    StructField("timestamp",   DoubleType()),
    StructField("ingested_at", DoubleType()),
])

def spark_session():
    return (SparkSession.builder
        .appName("SASAAnalytics")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate())

def run():
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("json").schema(SCHEMA)
        .option("path", str(BASE / "data" / "events"))
        .option("maxFilesPerTrigger", 20)
        .load()
        .withColumn("event_ts", F.to_timestamp(F.col("timestamp").cast(LongType())))
    )

    # 1. Page stats per project per 1-min window
    page_stats = (raw
        .filter(F.col("event_name") == "page_view")
        .withWatermark("event_ts", "2 minutes")
        .groupBy(F.window("event_ts", "1 minute"), "project_id", "path")
        .agg(F.count("*").alias("views"),
             F.approx_count_distinct("session_id").alias("sessions"),
             F.approx_count_distinct("user_id").alias("users"))
        .select(F.col("window.start").alias("window_start"), "project_id", "path", "views", "sessions", "users")
    )

    # 2. Event counts per type per project per window
    event_counts = (raw
        .withWatermark("event_ts", "2 minutes")
        .groupBy(F.window("event_ts", "1 minute"), "project_id", "event_name")
        .agg(F.count("*").alias("count"))
        .select(F.col("window.start").alias("window_start"), "project_id", "event_name", "count")
    )

    # 3. Session stats per project per window
    session_stats = (raw
        .withWatermark("event_ts", "2 minutes")
        .groupBy(F.window("event_ts", "1 minute"), "project_id")
        .agg(F.approx_count_distinct("session_id").alias("unique_sessions"),
             F.approx_count_distinct("user_id").alias("unique_users"),
             F.count("*").alias("total_events"))
        .select(F.col("window.start").alias("window_start"), "project_id",
                "unique_sessions", "unique_users", "total_events")
    )

    ckpt = str(BASE / "checkpoints")
    out  = str(BASE / "output")

    for df, name in [(page_stats,"page_stats"),(event_counts,"event_counts"),(session_stats,"session_stats")]:
        (df.writeStream.outputMode("append").format("json")
            .option("path", f"{out}/{name}")
            .option("checkpointLocation", f"{ckpt}/{name}")
            .trigger(processingTime="10 seconds")
            .start())

    print("Spark streaming job started.")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    run()
