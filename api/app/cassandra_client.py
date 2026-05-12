import os
import time
from cassandra.cluster import Cluster

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "wiki_analytics")


def get_cassandra_client():
    for attempt in range(20):
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            session = cluster.connect(CASSANDRA_KEYSPACE)
            return cluster, session
        except Exception:
            if attempt == 19:
                raise
            time.sleep(5)
