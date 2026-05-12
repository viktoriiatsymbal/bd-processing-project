import json
import os
import time
from datetime import datetime

from cassandra.cluster import Cluster
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "page-create-events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "cassandra-page-writer-group")

CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "cassandra").split(",")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "wiki_analytics")


def parse_ts(value):
    if value is None:
        return datetime.utcnow()

    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return datetime.utcnow()


class CassandraWriter:
    def __init__(self):
        for attempt in range(20):
            try:
                self.cluster = Cluster(CASSANDRA_HOSTS)
                self.session = self.cluster.connect(CASSANDRA_KEYSPACE)
                print("Connected to Cassandra", flush=True)
                break
            except Exception as exc:
                if attempt == 19:
                    raise
                print(f"Cassandra is not ready yet. Waiting... ({exc})", flush=True)
                time.sleep(5)

        self.insert_page_details = self.session.prepare("""
            INSERT INTO page_details (
                page_id,
                domain,
                page_title,
                user_id,
                user_name,
                is_bot,
                created_at,
                namespace_id,
                performer_is_registered
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        self.insert_pages_by_user = self.session.prepare("""
            INSERT INTO pages_by_user (
                user_id,
                created_at,
                page_id,
                domain,
                page_title,
                user_name,
                is_bot
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """)

        self.insert_pages_by_domain = self.session.prepare("""
            INSERT INTO pages_by_domain (
                domain,
                created_at,
                page_id,
                page_title,
                user_id,
                user_name,
                is_bot
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """)

        self.insert_domains_seen = self.session.prepare("""
            INSERT INTO domains_seen (
                bucket,
                domain,
                database,
                last_seen
            )
            VALUES (?, ?, ?, ?)
        """)

    def insert_event(self, event):
        created_at = parse_ts(event.get("created_at"))

        page_id = int(event["page_id"])
        domain = event["domain"].lower()
        page_title = event["page_title"]

        user_id = event.get("user_id")
        if user_id is not None:
            user_id = int(user_id)

        user_name = event.get("user_name") or "unknown"
        is_bot = bool(event.get("is_bot", False))
        performer_is_registered = bool(event.get("performer_is_registered", False))

        namespace_id = event.get("namespace_id")
        if namespace_id is not None:
            namespace_id = int(namespace_id)

        self.session.execute(
            self.insert_page_details,
            (
                page_id,
                domain,
                page_title,
                user_id,
                user_name,
                is_bot,
                created_at,
                namespace_id,
                performer_is_registered,
            ),
        )

        if user_id is not None:
            self.session.execute(
                self.insert_pages_by_user,
                (
                    user_id,
                    created_at,
                    page_id,
                    domain,
                    page_title,
                    user_name,
                    is_bot,
                ),
            )

        self.session.execute(
            self.insert_pages_by_domain,
            (
                domain,
                created_at,
                page_id,
                page_title,
                user_id,
                user_name,
                is_bot,
            ),
        )

        self.session.execute(
            self.insert_domains_seen,
            (
                "all",
                domain,
                event.get("database"),
                created_at,
            ),
        )

    def close(self):
        self.cluster.shutdown()


def build_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )


def main():
    consumer = build_consumer()
    writer = CassandraWriter()

    print("Cassandra page writer started", flush=True)

    try:
        for msg in consumer:
            event = msg.value
            writer.insert_event(event)
            print(
                "Inserted page:",
                event.get("domain"),
                event.get("page_id"),
                event.get("page_title"),
                flush=True,
            )
    finally:
        consumer.close()
        writer.close()


if __name__ == "__main__":
    main()
