import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "page-create-events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "minio-raw-writer-group")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "wikiuser")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "wikipass")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "wiki-events")

FLUSH_INTERVAL_SECONDS = int(os.getenv("FLUSH_INTERVAL_SECONDS", "60"))


def build_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )


def build_s3_client():
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=MINIO_BUCKET)

    print(f"Connected to MinIO bucket: {MINIO_BUCKET}", flush=True)
    return s3


class MinuteGroupUploader:
    def __init__(self, s3_client):
        self.s3 = s3_client
        self.batches = defaultdict(list)

    @staticmethod
    def _parse_created_at(event):
        value = event.get("created_at")

        if value is None:
            return datetime.now(timezone.utc)

        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return datetime.now(timezone.utc)

    @classmethod
    def _minute_key(cls, event):
        dt = cls._parse_created_at(event)
        return dt.strftime("%Y%m%d_%H%M")

    @staticmethod
    def _object_path(minute_key):
        dt = datetime.strptime(minute_key, "%Y%m%d_%H%M")
        return f"raw/{dt.strftime('%Y/%m/%d/%H')}/page_create_{minute_key}.json"

    def add_event(self, event):
        minute_key = self._minute_key(event)
        self.batches[minute_key].append(event)

    def flush_ready_batches(self, flush_all=False):
        current_minute = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")

        if flush_all:
            keys_to_flush = list(self.batches.keys())
        else:
            keys_to_flush = [
                minute_key
                for minute_key in self.batches
                if minute_key != current_minute
            ]

        for minute_key in keys_to_flush:
            events = self.batches.pop(minute_key, [])

            if not events:
                continue

            body = "\n".join(
                json.dumps(event, ensure_ascii=False)
                for event in events
            ).encode("utf-8")

            object_key = self._object_path(minute_key)

            self.s3.put_object(
                Bucket=MINIO_BUCKET,
                Key=object_key,
                Body=body,
                ContentType="application/json",
            )

            print(
                f"Uploaded {len(events)} raw events to s3://{MINIO_BUCKET}/{object_key}",
                flush=True,
            )


def main():
    consumer = build_consumer()
    s3 = build_s3_client()
    uploader = MinuteGroupUploader(s3)

    last_flush = time.time()

    print("MinIO raw writer started", flush=True)

    try:
        for msg in consumer:
            uploader.add_event(msg.value)

            now = time.time()
            if now - last_flush >= FLUSH_INTERVAL_SECONDS:
                uploader.flush_ready_batches(flush_all=False)
                last_flush = now

    finally:
        uploader.flush_ready_batches(flush_all=True)
        consumer.close()


if __name__ == "__main__":
    main()
