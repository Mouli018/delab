"""
Kafka Consumer — idempotently applies CDC events to the SQLite warehouse.
============================================================================
Requires a running broker + `pip install kafka-python`.

This is the piece that actually proves idempotency under Kafka's
at-least-once delivery guarantee: every event is applied via an
INSERT...ON CONFLICT upsert keyed on product_id, AND the event_id itself is
recorded in a dedup table first — so a redelivered message (consumer
restart, rebalance, broker retry) is a guaranteed no-op, not a duplicate
write or a double-decrement on a DELETE.

Run:
    python3 consumer.py
Kill it (Ctrl+C) mid-stream and restart - it resumes from the last committed
offset via the consumer group `warehouse-loader`, and re-processing any
in-flight redelivered messages will not change row counts.
"""
import json
import sqlite3
from pathlib import Path

from kafka import KafkaConsumer

BASE = Path(__file__).resolve().parents[2]
WAREHOUSE_DB = BASE / "data" / "warehouse" / "resilient_warehouse.db"
TOPIC = "product-cdc-events"
BOOTSTRAP_SERVERS = ["localhost:9092"]
CONSUMER_GROUP = "warehouse-loader"

DDL = """
CREATE TABLE IF NOT EXISTS products_streaming (
    product_id INTEGER PRIMARY KEY,
    articleType TEXT, baseColour TEXT, season TEXT, usage TEXT,
    productDisplayName TEXT,
    last_event_type TEXT, last_updated TEXT
);
CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    processed_at TEXT DEFAULT CURRENT_TIMESTAMP
);
"""


def get_conn():
    conn = sqlite3.connect(WAREHOUSE_DB)
    conn.executescript(DDL)
    return conn


def apply_event(conn, event: dict) -> str:
    """Returns 'applied' or 'duplicate_skipped'."""
    cur = conn.cursor()

    # Idempotency gate #1: have we already processed this exact event_id?
    cur.execute("SELECT 1 FROM processed_events WHERE event_id = ?", (event["event_id"],))
    if cur.fetchone():
        return "duplicate_skipped"

    payload = event["payload"]
    if event["event_type"] in ("INSERT", "UPDATE"):
        # Idempotency gate #2: even a *new* event_id representing a state
        # that's already current results in a harmless overwrite, not a
        # duplicate row, because this is an UPSERT keyed on product_id
        cur.execute("""
            INSERT INTO products_streaming
                (product_id, articleType, baseColour, season, usage,
                 productDisplayName, last_event_type, last_updated)
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(product_id) DO UPDATE SET
                articleType=excluded.articleType, baseColour=excluded.baseColour,
                season=excluded.season, usage=excluded.usage,
                productDisplayName=excluded.productDisplayName,
                last_event_type=excluded.last_event_type,
                last_updated=excluded.last_updated
        """, (event["product_id"], payload["articleType"], payload["baseColour"],
              payload["season"], payload["usage"], payload["productDisplayName"],
              event["event_type"], event["event_ts"]))
    elif event["event_type"] == "DELETE":
        cur.execute("DELETE FROM products_streaming WHERE product_id = ?",
                    (event["product_id"],))

    cur.execute("INSERT INTO processed_events (event_id) VALUES (?)", (event["event_id"],))
    conn.commit()
    return "applied"


def run_consumer():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,          # commit offsets after processing, not before
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        consumer_timeout_ms=30000,        # for demo runs: stop after 30s of no new messages
    )
    conn = get_conn()

    applied, skipped = 0, 0
    print(f"[consumer] listening on '{TOPIC}' as group '{CONSUMER_GROUP}'...")
    try:
        for message in consumer:
            event = message.value
            result = apply_event(conn, event)
            if result == "applied":
                applied += 1
            else:
                skipped += 1
            if (applied + skipped) % 50 == 0:
                print(f"[consumer] applied={applied} duplicate_skipped={skipped} "
                      f"(partition={message.partition}, offset={message.offset})")
    except KeyboardInterrupt:
        print("[consumer] stopped by user")
    finally:
        conn.close()
        consumer.close()
        print(f"[consumer] final: applied={applied}, duplicate_skipped={skipped}")


if __name__ == "__main__":
    run_consumer()
