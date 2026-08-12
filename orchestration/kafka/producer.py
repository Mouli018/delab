"""
Kafka Producer — streams simulated CDC events from the product catalog.
============================================================================
Requires a running broker (see docker-compose.yml) and `pip install kafka-python`.

Run:
    python3 producer.py                  # streams a mix of inserts/updates/deletes
    python3 producer.py --replay-all     # streams every row as an INSERT (initial load)
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

BASE = Path(__file__).resolve().parents[2]
CATALOG_CSV = BASE / "week1_eda" / "outputs" / "cleaned_data.csv"
TOPIC = "product-cdc-events"
BOOTSTRAP_SERVERS = ["localhost:9092"]


def make_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",              # wait for full ISR ack - durability over raw throughput
        retries=5,
        linger_ms=20,
    )


def build_event(row, event_type):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "product_id": int(row["id"]),
        "payload": {
            "articleType": row["articleType"],
            "baseColour": row["baseColour"],
            "season": row["season"],
            "usage": row["usage"],
            "productDisplayName": row["productDisplayName"],
        },
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }


def stream_events(replay_all=False, sample_size=500, delay_seconds=0.05):
    df = pd.read_csv(CATALOG_CSV)
    producer = make_producer()
    sent = 0

    try:
        if replay_all:
            # full initial load - every row as an INSERT, keyed by product_id
            # so all events for a product land on the same partition/order
            for _, row in df.iterrows():
                event = build_event(row, "INSERT")
                producer.send(TOPIC, key=event["product_id"], value=event)
                sent += 1
                if sent % 5000 == 0:
                    producer.flush()
                    print(f"[producer] {sent:,} INSERT events sent")
        else:
            # steady-state simulation: a mix of inserts/updates/deletes on a
            # random sample, with a small delay between messages to mimic a
            # live change stream rather than a bulk dump
            sample = df.sample(n=min(sample_size, len(df)), random_state=None)
            for _, row in sample.iterrows():
                event_type = random.choices(
                    ["INSERT", "UPDATE", "DELETE"], weights=[0.2, 0.7, 0.1]
                )[0]
                event = build_event(row, event_type)
                future = producer.send(TOPIC, key=event["product_id"], value=event)
                try:
                    future.get(timeout=5)  # confirm delivery for this demo run
                except KafkaError as e:
                    print(f"[producer] delivery failed for product {event['product_id']}: {e}")
                    continue
                sent += 1
                if sent % 50 == 0:
                    print(f"[producer] {sent} events streamed (latest: {event_type} "
                          f"product_id={event['product_id']})")
                time.sleep(delay_seconds)
    finally:
        producer.flush()
        producer.close()
        print(f"[producer] done. {sent} events sent to topic '{TOPIC}'.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--replay-all", action="store_true",
                         help="Stream the full catalog as INSERT events (initial load)")
    parser.add_argument("--sample-size", type=int, default=500)
    args = parser.parse_args()
    stream_events(replay_all=args.replay_all, sample_size=args.sample_size)
