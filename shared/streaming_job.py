from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, IntegerType,
    LongType, StringType,
    StructField, StructType)

KAFKA_BOOTSTRAP = "kafka:29092"

INPUT_TOPIC = "page-create-events"
BREAKING_ALERT_TOPIC = "breaking-news-alerts"
BOT_ALERT_TOPIC = "bot-alerts"
SPAM_ALERT_TOPIC = "spam-alerts"

CASSANDRA_HOST = "cassandra"
CASSANDRA_KEYSPACE = "wiki_analytics"

CHECKPOINT_LOCATION = "/tmp/spark-checkpoints/wiki-streaming"

spark = (
    SparkSession.builder
    .appName("wikipedia-real-time-analytics")
    .config("spark.cassandra.connection.host", CASSANDRA_HOST)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("domain", StringType(), True),
    StructField("database", StringType(), True),
    StructField("page_id", LongType(), True),
    StructField("page_title", StringType(), True),
    StructField("user_id", LongType(), True),
    StructField("user_name", StringType(), True),
    StructField("is_bot", BooleanType(), True),
    StructField("performer_is_registered", BooleanType(), True),
    StructField("user_edit_count", LongType(), True),
    StructField("namespace_id", IntegerType(), True),
    StructField("created_at", StringType(), True),
    StructField("ingested_at", StringType(), True),
])

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .load())

events = (
    raw_stream
    .select(F.col("value").cast("string").alias("json_value"))
    .select(F.from_json(F.col("json_value"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_timestamp", F.to_timestamp("created_at"))
    .withColumn("domain", F.lower(F.trim(F.col("domain"))))
    .withColumn("page_title", F.trim(F.col("page_title")))
    .withColumn("user_name", F.coalesce(F.col("user_name"), F.lit("unknown")))
    .withColumn("is_bot", F.coalesce(F.col("is_bot"), F.lit(False)))
    .withColumn("performer_is_registered", F.coalesce(F.col("performer_is_registered"), F.lit(False)))
    .withColumn("user_edit_count", F.coalesce(F.col("user_edit_count"), F.lit(0)))
    .withColumn(
        "user_key",
        F.coalesce(F.col("user_id").cast("string"), F.col("user_name"), F.lit("unknown")))
    .where(F.col("event_timestamp").isNotNull())
    .where(F.col("domain").isNotNull())
    .where(F.col("page_id").isNotNull())
    .where(F.col("page_title").isNotNull()))

def send_stream_to_kafka(df, topic, checkpoint_name, output_mode="update"):
    return (
        df
        .select(F.to_json(F.struct(*[F.col(c) for c in df.columns])).alias("value"))
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", topic)
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/{checkpoint_name}")
        .outputMode(output_mode)
        .trigger(processingTime="30 seconds").start())

def send_batch_to_kafka(df, topic):
    if df.rdd.isEmpty():
        return

    (df.select(F.to_json(F.struct(*[F.col(c) for c in df.columns])).alias("value"))
    .write.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", topic).save())

def build_breaking_news_alerts(events):

    # activity spike detection
    hour_windowed_events = (
        events
        .withWatermark("event_timestamp", "2 minutes")
        .withColumn(
            "hour_window",
            F.window(F.col("event_timestamp"), "1 hour", "5 minutes")))

    is_last_5min_of_hour_window = (
        F.col("event_timestamp") >= F.expr("hour_window.end - INTERVAL 5 MINUTES"))

    activity_spikes = (
        hour_windowed_events
        .groupBy("domain", "hour_window")
        .agg(
            F.count("*").cast("long").alias("pages_last_hour"),
            F.sum(
                F.when(is_last_5min_of_hour_window, 1).otherwise(0)
            ).cast("long").alias("pages_last_5min"),
            F.collect_list(
                F.when(is_last_5min_of_hour_window, F.col("page_title"))
            ).alias("sample_pages_raw"),
        )
        .withColumn(
            "avg_pages_per_5min",
            F.round(F.col("pages_last_hour") / F.lit(12.0), 2))
        .withColumn(
            "spike_ratio",
            F.when(
                F.col("avg_pages_per_5min") > 0,
                F.round(F.col("pages_last_5min") / F.col("avg_pages_per_5min"), 2)
            ).otherwise(F.lit(0.0)))
        .where(F.col("spike_ratio") > 3.0)
        .withColumn(
            "sample_pages",
            F.expr("slice(filter(sample_pages_raw, x -> x is not null), 1, 5)"))
        .withColumn("alert_time", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("alert_type", F.lit("activity_spike"))
        .withColumn("window_start", F.col("hour_window.start"))
        .withColumn("window_end", F.col("hour_window.end"))
        .select(
            "alert_time",
            "alert_type",
            "domain",
            "window_start",
            "window_end",
            "pages_last_5min",
            "avg_pages_per_5min",
            "spike_ratio",
            "sample_pages"))

    # keyword burst detection
    stop_words = [
    # English
    "the", "and", "of", "in", "to", "a", "an", "on", "for", "with", "by", "is",
    "are", "was", "were", "from", "at", "as", "it", "this", "that",

    # Ukrainian
    "і", "та", "у", "в", "на", "з", "до", "для", "що", "це", "як", "за", "про",
    "від", "або", "але", "його", "її", "їх",

    # German
    "der", "die", "das", "und", "in", "zu", "den", "von", "mit", "auf", "für",
    "ist", "im", "dem", "ein", "eine",

    # French
    "le", "la", "les", "de", "des", "du", "et", "en", "un", "une", "pour",
    "dans", "sur", "avec", "est",

    # Spanish
    "el", "la", "los", "las", "de", "del", "y", "en", "un", "una", "para",
    "con", "por", "es",

    # Italian
    "il", "lo", "la", "gli", "le", "di", "del", "della", "e", "in", "un",
    "una", "per", "con",

    # Polish
    "i", "w", "na", "z", "do", "dla", "że", "to", "jest", "się", "nie", "od",

    # Portuguese
    "o", "a", "os", "as", "de", "do", "da", "e", "em", "um", "uma", "para",
    "com", "por"]

    keyword_tokens = (
        events
        .withWatermark("event_timestamp", "2 minutes")
        .withColumn(
            "clean_title",
            F.lower(F.regexp_replace("page_title", r"[^\p{L}\p{N} ]", " ")))
        .withColumn("keyword", F.explode(F.split("clean_title", r"\s+")))
        .where(F.length("keyword") >= 3)
        .where(~F.col("keyword").isin(stop_words))
        .withColumn(
            "keyword_window",
            F.window(F.col("event_timestamp"), "10 minutes", "1 minute"))
        .select(
            "keyword_window",
            "keyword",
            "page_id",
            "domain",
            "page_title")
        .dropDuplicates(["keyword_window", "keyword", "page_id"]))

    keyword_bursts = (
        keyword_tokens
        .groupBy("keyword_window", "keyword")
        .agg(
            F.count("*").cast("long").alias("occurrences"),
            F.collect_set("domain").alias("domains"),
            F.slice(F.collect_set("page_title"), 1, 5).alias("sample_pages"),
        )
        .where(F.col("occurrences") >= 5)
        .withColumn("alert_time", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("alert_type", F.lit("keyword_burst"))
        .withColumn("window_start", F.col("keyword_window.start"))
        .withColumn("window_end", F.col("keyword_window.end"))
        .select(
            "alert_time",
            "alert_type",
            "keyword",
            "window_start",
            "window_end",
            "occurrences",
            "domains",
            "sample_pages"))
    return activity_spikes, keyword_bursts

def build_user_minute_metrics(events):
    return (
        events
        .withWatermark("event_timestamp", "2 minutes")
        .groupBy(
            F.window(F.col("event_timestamp"), "1 minute"),
            F.col("domain"),
            F.col("user_key"),
            F.col("user_id"),
            F.col("user_name"),
            F.col("is_bot"))
        .agg(
            F.count("*").cast("long").alias("pages_created_by_user"),
            F.sum(F.length("page_title")).cast("long").alias("title_length_sum")))

def write_language_activity(batch_df):
    language_activity = (
        batch_df
        .groupBy("domain", "minute_start")
        .agg(
            F.sum("pages_created_by_user").cast("long").alias("pages_created"),
            F.countDistinct("user_key").cast("long").alias("unique_authors"),
            F.round(
                F.sum("title_length_sum") / F.sum("pages_created_by_user"),
                2
            ).alias("avg_title_length")))

    trend_window = Window.partitionBy("domain").orderBy("minute_start")

    language_activity = (
        language_activity
        .withColumn("prev_pages", F.lag("pages_created").over(trend_window))
        .withColumn(
            "trend_vs_prev_minute",
            F.when(F.col("prev_pages").isNull(), F.lit(0.0))
            .when(F.col("prev_pages") == 0, F.lit(0.0))
            .otherwise(
                F.round(
                    (F.col("pages_created") - F.col("prev_pages")) /
                    F.col("prev_pages") * 100.0, 2))).drop("prev_pages"))

    (language_activity.write
     .format("org.apache.spark.sql.cassandra")
     .mode("append")
     .options(table="language_activity", keyspace=CASSANDRA_KEYSPACE).save())

def write_bot_activity(batch_df):
    bot_totals = (
        batch_df
        .groupBy("domain", "minute_start")
        .agg(
            F.sum(
                F.when(F.col("is_bot"), F.col("pages_created_by_user")).otherwise(0)
            ).cast("long").alias("bot_pages"),
            F.sum(
                F.when(~F.col("is_bot"), F.col("pages_created_by_user")).otherwise(0)
            ).cast("long").alias("human_pages"),
        )
        .withColumn(
            "bot_percent",
            F.when(
                (F.col("bot_pages") + F.col("human_pages")) > 0,
                F.round(
                    F.col("bot_pages") /
                    (F.col("bot_pages") + F.col("human_pages")) * 100.0, 2)).otherwise(F.lit(0.0))))

    rank_window = Window.partitionBy(
        "domain",
        "minute_start",
        "is_bot"
    ).orderBy(F.desc("pages_created_by_user"), F.asc("user_name"))

    ranked_users = (
        batch_df
        .withColumn("rn", F.row_number().over(rank_window))
        .where(F.col("rn") <= 5))

    top_bots = (
        ranked_users
        .where(F.col("is_bot"))
        .groupBy("domain", "minute_start")
        .agg(
            F.to_json(
                F.collect_list(
                    F.struct(
                        F.col("user_name").alias("name"),
                        F.col("pages_created_by_user").alias("pages"),
                        F.col("is_bot").alias("is_bot")))).alias("top_bots")))

    top_humans = (
        ranked_users
        .where(~F.col("is_bot"))
        .groupBy("domain", "minute_start")
        .agg(
            F.to_json(
                F.collect_list(
                    F.struct(
                        F.col("user_name").alias("name"),
                        F.col("pages_created_by_user").alias("pages"),
                        F.col("is_bot").alias("is_bot")))).alias("top_humans")))

    bot_activity_metrics = (
        bot_totals
        .join(top_bots, on=["domain", "minute_start"], how="left")
        .join(top_humans, on=["domain", "minute_start"], how="left")
        .withColumn("top_bots", F.coalesce(F.col("top_bots"), F.lit("[]")))
        .withColumn("top_humans", F.coalesce(F.col("top_humans"), F.lit("[]"))))

    (bot_activity_metrics.write
     .format("org.apache.spark.sql.cassandra")
     .mode("append")
     .options(table="bot_activity_metrics", keyspace=CASSANDRA_KEYSPACE).save())

    high_bot_alerts = (
        bot_activity_metrics
        .where(F.col("bot_percent") > 80)
        .withColumn("alert_time", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("alert_type", F.lit("high_bot_activity"))
        .select(
            "alert_time",
            "alert_type",
            "domain",
            "minute_start",
            "bot_pages",
            "human_pages",
            "bot_percent",
            "top_bots"))

    send_batch_to_kafka(high_bot_alerts, BOT_ALERT_TOPIC)

def write_minute_metrics(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    print(f"Writing minute metrics batch {batch_id}", flush=True)

    batch_df = (
        batch_df
        .withColumn("minute_start", F.col("window.start"))
        .drop("window")
        .cache())
    write_language_activity(batch_df)
    write_bot_activity(batch_df)
    batch_df.unpersist()

def build_single_bot_alerts(events):
    return (
        events
        .where(F.col("is_bot"))
        .withWatermark("event_timestamp", "2 minutes")
        .groupBy(
            F.window(F.col("event_timestamp"), "10 minutes", "1 minute"),
            F.col("domain"),
            F.col("user_name"))
        .agg(F.count("*").cast("long").alias("pages_created"))
        .where(F.col("pages_created") > 50)
        .withColumn("alert_time", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("alert_type", F.lit("single_bot_many_pages"))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .select(
            "alert_time",
            "alert_type",
            "domain",
            "user_name",
            "window_start",
            "window_end",
            "pages_created"))

def build_spam_alerts(events):
    non_bot_user_many_pages_alerts = (
        events
        .where(~F.col("is_bot"))
        .withWatermark("event_timestamp", "2 minutes")
        .groupBy(
            F.window(F.col("event_timestamp"), "5 minutes", "1 minute"),
            F.col("user_key"),
            F.col("user_id"),
            F.col("user_name"))
        .agg(
            F.count("*").cast("long").alias("pages_created"),
            F.collect_set("domain").alias("domains"),
        )
        .where(F.col("pages_created") > 10)
        .withColumn("alert_time", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("alert_type", F.lit("non_bot_many_pages"))
        .withColumn("severity", F.lit("high"))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .select(
            "alert_time",
            "alert_type",
            "severity",
            "user_id",
            "user_name",
            "window_start",
            "window_end",
            "pages_created",
            "domains"))

    weird_title_alerts = (
        events
        .withColumn("title_length", F.length("page_title"))
        .where(
            (F.col("page_title").rlike(r"https?://|www\.|\+?\d[\d\s().-]{7,}")) |
            (F.col("page_title").rlike(r".*\d.*\d.*\d.*\d.*\d.*")) |
            (F.col("title_length") < 3) |
            (F.col("title_length") > 100))
        .withColumn("alert_time", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("alert_type", F.lit("weird_title"))
        .withColumn(
            "severity",
            F.when(
                (F.col("title_length") < 3) | (F.col("title_length") > 100),
                F.lit("medium")
            ).otherwise(F.lit("high")))
        .select(
            "alert_time",
            "alert_type",
            "severity",
            "domain",
            "page_id",
            "page_title",
            "user_id",
            "user_name",
            "title_length"))

    new_user_domain_events = (
        events
        .where(
            (F.col("performer_is_registered") == False) |
            (F.col("user_edit_count") <= 1))
        .withWatermark("event_timestamp", "2 minutes")
        .withColumn(
            "new_user_window",
            F.window(F.col("event_timestamp"), "10 minutes", "1 minute"))
        .select(
            "new_user_window",
            "user_key",
            "user_name",
            "domain",
            "event_timestamp")
        .dropDuplicates(["new_user_window", "user_key", "domain"]))

    new_user_multi_domain_alerts = (
        new_user_domain_events
        .groupBy("new_user_window", "user_key", "user_name")
        .agg(
            F.count("*").cast("long").alias("domains_count"),
            F.collect_set("domain").alias("domains"),
        )
        .where(F.col("domains_count") >= 2)
        .withColumn("alert_time", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("alert_type", F.lit("new_user_multi_domain"))
        .withColumn("severity", F.lit("medium"))
        .withColumn("window_start", F.col("new_user_window.start"))
        .withColumn("window_end", F.col("new_user_window.end"))
        .select(
            "alert_time",
            "alert_type",
            "severity",
            "user_name",
            "window_start",
            "window_end",
            "domains_count",
            "domains"))

    return (
        non_bot_user_many_pages_alerts,
        weird_title_alerts,
        new_user_multi_domain_alerts,
    )

activity_spikes, keyword_bursts = build_breaking_news_alerts(events)
per_user_minute = build_user_minute_metrics(events)
single_bot_alerts = build_single_bot_alerts(events)

non_bot_user_many_pages_alerts, weird_title_alerts, new_user_multi_domain_alerts = build_spam_alerts(events)

minute_metrics_query = (
    per_user_minute
    .writeStream
    .foreachBatch(write_minute_metrics)
    .outputMode("complete")
    .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/minute_metrics")
    .trigger(processingTime="30 seconds")
    .start())

activity_spike_query = send_stream_to_kafka(
    activity_spikes,
    BREAKING_ALERT_TOPIC,
    "activity_spikes",
    output_mode="update")

keyword_burst_query = send_stream_to_kafka(
    keyword_bursts,
    BREAKING_ALERT_TOPIC,
    "keyword_bursts",
    output_mode="update")

single_bot_query = send_stream_to_kafka(
    single_bot_alerts,
    BOT_ALERT_TOPIC,
    "single_bot_alerts",
    output_mode="update")

non_bot_user_many_pages_query = send_stream_to_kafka(
    non_bot_user_many_pages_alerts,
    SPAM_ALERT_TOPIC,
    "non_bot_user_many_pages",
    output_mode="update")

weird_title_query = send_stream_to_kafka(
    weird_title_alerts,
    SPAM_ALERT_TOPIC,
    "weird_titles",
    output_mode="append")

new_user_multi_domain_query = send_stream_to_kafka(
    new_user_multi_domain_alerts,
    SPAM_ALERT_TOPIC,
    "new_user_multi_domain",
    output_mode="update")

spark.streams.awaitAnyTermination()
