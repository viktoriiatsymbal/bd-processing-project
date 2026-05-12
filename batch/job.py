from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "wikiuser"
MINIO_SECRET_KEY = "wikipass"
INPUT_PATH = "s3a://wiki-events/raw/*/*/*/*/*.json"
HOURLY_OUTPUT_PATH = "s3a://wiki-events/output/hourly_activity_report/"
EDITOR_OUTPUT_PATH = "s3a://wiki-events/output/editor_behavior_patterns/"
CASSANDRA_HOST = "cassandra"
KEYSPACE = "wiki_analytics"

spark = (
    SparkSession.builder.appName("wikipedia-batch-analytics")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.cassandra.connection.host", CASSANDRA_HOST)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

def filter_last_full_hours(df, hours=6):
    current_hour = F.date_trunc("hour", F.current_timestamp())
    start_hour = current_hour - F.expr(f"INTERVAL {hours} HOURS")

    return df.where(
        (F.col("event_timestamp") >= start_hour) &
        (F.col("event_timestamp") < current_hour)
    )

def load_data():
    df = spark.read.json(INPUT_PATH)
    return (
        df.withColumn("event_timestamp", F.to_timestamp("created_at"))
        .filter(F.col("event_timestamp").isNotNull())
        .filter(F.col("domain").isNotNull())
        .filter(F.col("page_id").isNotNull())
        .withColumn("domain", F.lower(F.trim(F.col("domain"))))
        .withColumn("page_title", F.trim(F.col("page_title")))
        .withColumn("user_name", F.coalesce(F.col("user_name"), F.lit("unknown")))
        .withColumn("is_bot", F.coalesce(F.col("is_bot"), F.lit(False)))
        .withColumn(
            "user_key",
            F.coalesce(F.col("user_id").cast("string"), F.col("user_name"), F.lit("unknown"))
        )
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("event_hour", F.hour("event_timestamp"))
        .withColumn("hour_start", F.date_trunc("hour", F.col("event_timestamp")))
        .withColumn("hour_end", F.expr("hour_start + interval 1 hour"))
    )


def build_hourly_activity_report(df):
    top_author_counts = (
        df.groupBy("domain", "hour_start", "user_name", "is_bot")
        .agg(F.count("*").alias("pages"))
    )
    author_rank = Window.partitionBy("domain", "hour_start").orderBy(F.desc("pages"), F.asc("user_name"))
    top_authors = (
        top_author_counts.withColumn("rn", F.row_number().over(author_rank))
        .where(F.col("rn") <= 10)
        .groupBy("domain", "hour_start")
        .agg(F.to_json(F.collect_list(F.struct(
            # "user_name", "pages", "is_bot"
            F.col("user_name").alias("name"),
                        F.col("pages"),
                        F.col("is_bot")
            ))).alias("top_authors"))
    )

    namespace_counts = (
        df.groupBy("domain", "hour_start", "namespace_id")
        .agg(F.count("*").alias("pages"))
    )

    pages_by_namespace = (
        namespace_counts
        .groupBy("domain", "hour_start")
        .agg(
            F.to_json(
                F.collect_list(
                    F.struct(
                        F.col("namespace_id"),
                        F.col("pages")
                    )
                )
            ).alias("pages_by_namespace")
        )
    )

    hourly = (
        df.groupBy("domain", "hour_start", "hour_end")
        .agg(
            F.count("*").cast("long").alias("pages_created"),
            F.countDistinct("user_key").cast("long").alias("unique_authors"),
            # F.countDistinct("user_id").cast("long").alias("unique_authors"),
            F.sum(F.when(F.col("is_bot"), 1).otherwise(0)).cast("long").alias("bot_pages"),
            F.sum(F.when(~F.col("is_bot"), 1).otherwise(0)).cast("long").alias("human_pages"),
        )
        .withColumn(
            "bot_percent",
            F.when((F.col("bot_pages") + F.col("human_pages")) > 0,
                   F.round(F.col("bot_pages") / (F.col("bot_pages") + F.col("human_pages")) * 100.0, 2))
             .otherwise(F.lit(0.0))
        )
        .join(top_authors, on=["domain", "hour_start"], how="left")
        .join(pages_by_namespace, on=["domain", "hour_start"], how="left")
        .withColumn("top_authors", F.coalesce(F.col("top_authors"), F.lit("[]")))
        .withColumn("pages_by_namespace", F.coalesce(F.col("pages_by_namespace"), F.lit("[]")))
    )

    hourly.write.format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="hourly_activity_by_domain", keyspace=KEYSPACE).save()
    hourly.write.mode("overwrite").json(HOURLY_OUTPUT_PATH)
    print("B1. Hourly Activity Report is saved to Cassandra and MinIO")


def build_editor_behavior_patterns(df):
    user_events = df.where(F.col("user_id").isNotNull())
    time_window = Window.partitionBy("user_id").orderBy("event_timestamp")
    with_prev = (
        user_events.withColumn("prev_time", F.lag("event_timestamp").over(time_window))
        .withColumn(
            "minutes_since_prev",
            F.when(F.col("prev_time").isNotNull(),
                   (F.col("event_timestamp").cast("long") - F.col("prev_time").cast("long")) / 60.0)
        )
    )

    domain_counts = user_events.groupBy("user_id", "domain").agg(F.count("*").alias("domain_pages"))
    domain_rank = Window.partitionBy("user_id").orderBy(F.desc("domain_pages"), F.asc("domain"))
    dominant_domain = (
        domain_counts.withColumn("rn", F.row_number().over(domain_rank))
        .where(F.col("rn") == 1)
        .select("user_id", F.col("domain").alias("dominant_domain"))
    )

    active_hours = (
        user_events.groupBy("user_id")
        .agg(F.to_json(F.sort_array(F.collect_set("event_hour"))).alias("active_hours"))
    )

    editor_patterns = (
        with_prev.groupBy("user_id")
        .agg(
            F.first("user_name", ignorenulls=True).alias("user_name"),
            F.count("*").cast("long").alias("pages_created"),
            F.round(F.avg("minutes_since_prev"), 2).alias("avg_minutes_between_pages"),
            F.countDistinct("domain").cast("long").alias("domains_count"),
        )
        .where(F.col("pages_created") > 5)
        .join(active_hours, on="user_id", how="left")
        .join(dominant_domain, on="user_id", how="left")
        .withColumn("updated_at", F.current_timestamp())
        .select(
            "user_id", "user_name", "pages_created", "avg_minutes_between_pages",
            "active_hours", "domains_count", "dominant_domain", "updated_at"
        )
    )

    editor_patterns.write.format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="editor_behavior_patterns", keyspace=KEYSPACE).save()
    editor_patterns.write.mode("overwrite").parquet(EDITOR_OUTPUT_PATH)
    print("B2. Editor Behavior Patterns are saved to Cassandra and MinIO")


def main():
    df = load_data().cache()
    hourly_df = filter_last_full_hours(df, hours=6).cache()

    print(f"Rows after preprocessing: {df.count()}")
    print(f"Rows for last 6 full hours: {hourly_df.count()}")

    build_hourly_activity_report(hourly_df)
    build_editor_behavior_patterns(df)

    hourly_df.unpersist()
    df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
