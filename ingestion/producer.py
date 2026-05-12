import json
import os
import time
from datetime import datetime, timezone
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "page-create-events")
WIKIMEDIA_STREAM_URL = os.getenv(
    "WIKIMEDIA_STREAM_URL",
    "https://stream.wikimedia.org/v2/stream/page-create")

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8"),
                acks="all",
                retries=5)
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}", flush=True)
            return producer
        except NoBrokersAvailable:
            print("Kafka is not ready yet. Waiting...", flush=True)
            time.sleep(5)

def clean_event(raw):
    meta_data = raw.get("meta") or {}
    user_data = raw.get("performer") or {}
    domain = meta_data.get("domain")
    database = raw.get("database")
    page_id = raw.get("page_id")
    page_title = raw.get("page_title")
    user_id = user_data.get("user_id")
    user_name = user_data.get("user_text")
    is_bot = bool(user_data.get("user_is_bot", False))
    user_edit_count = user_data.get("user_edit_count")
    namespace_id = raw.get("page_namespace")
    created_at = raw.get("dt") or raw.get("rev_timestamp") or meta_data.get("dt")
    user_registration_dt = user_data.get("user_registration_dt")

    if meta_data.get("domain") == "canary":
        return None
    if page_id is None or page_title is None:
        return None

    return {
        "domain": domain,
        "database": database,
        "page_id": int(page_id),
        "page_title": page_title,
        "user_id": user_id,
        "user_name": user_name,
        "is_bot": is_bot,
        "performer_is_registered": user_registration_dt is not None,
        "user_edit_count": user_edit_count,
        "namespace_id": namespace_id,
        "created_at": created_at,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }

def read_stream():
    with requests.get(WIKIMEDIA_STREAM_URL, headers={
        "Accept": "text/event-stream", "User-Agent": "wiki-analytics-project/1.0"},
        stream=True, timeout=90) as response:
        response.raise_for_status()
        for line in response.iter_lines(decode_unicode=True):
            if not line or not line.startswith("data: "):
                continue
            payload = line.replace("data: ", "", 1)
            try:
                raw_event = json.loads(payload)
                event = clean_event(raw_event)
                if event:
                    yield event
            except Exception as exc:
                print(f"Skipped invalid Wikimedia event: {exc}", flush=True)

def main():
    producer = create_producer()
    print(f"Sending page-create events to topic '{KAFKA_TOPIC}'", flush=True)
    counter = 0
    while True:
        try:
            for event in read_stream():
                producer.send(KAFKA_TOPIC, key=event["domain"], value=event)
                counter += 1
                if counter % 50 == 0:
                    producer.flush()
                    print(f"Sent {counter} Wikimedia events", flush=True)
        except Exception as exc:
            print(f"Wikimedia stream error: {exc}", flush=True)
            time.sleep(10)

if __name__ == "__main__":
    main()
