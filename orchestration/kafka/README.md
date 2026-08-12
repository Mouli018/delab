# Kafka — CDC Event Streaming Architecture

This isn't runnable inside the sandbox I built the rest of this project in — it
needs a real Kafka broker (a JVM service), and my network here is locked to
package registries only, no broker download or Docker daemon available. What's
here is **real, runnable code** against a broker you stand up yourself — a
`docker-compose.yml` is included so that's a 2-minute `docker compose up`.

## Why Kafka fits here

Week 2 already implements **CDC** (`week2_etl/pipeline_week2.py::run_cdc`) via
row-hash snapshot diffing — a *pull-based*, batch-oriented CDC. Kafka is the
natural *push-based*, streaming upgrade to that: instead of periodically
diffing two snapshots, every insert/update/delete on the product catalog is
published as an event the instant it happens, and downstream consumers apply
it incrementally. This is also where Week 5's **idempotency** requirement gets
sharpest — Kafka consumer groups can and do redeliver messages (at-least-once
delivery is the default), so the consumer *must* be idempotent or you get
duplicate warehouse rows on every consumer restart/rebalance.

## Architecture

```
                     ┌─────────────────────┐
  product catalog    │   Kafka Topic:       │      consumer group:
  change source  ───▶│  product-cdc-events  │───▶  warehouse-loader
  (producer.py)      │  (3 partitions,       │      (consumer.py)
                      │   keyed by product_id)│           │
                      └─────────────────────┘            ▼
                                                   SQLite warehouse
                                                   (idempotent UPSERT
                                                    on product_id)
```

- **Topic:** `product-cdc-events`, 3 partitions — keyed by `product_id` so all
  events for the same product land on the same partition and are processed
  **in order** relative to each other (Kafka only guarantees ordering within a
  partition, not across the whole topic).
- **Message schema** (JSON):
  ```json
  {
    "event_id": "uuid",
    "event_type": "INSERT | UPDATE | DELETE",
    "product_id": 15970,
    "payload": { "articleType": "Shirts", "baseColour": "Navy Blue", ... },
    "event_ts": "2026-08-10T05:00:00Z"
  }
  ```
- **Consumer group:** `warehouse-loader` — a single logical consumer (can be
  scaled to multiple instances across the 3 partitions for throughput).
- **Idempotency:** the consumer applies every event via `INSERT ... ON
  CONFLICT(product_id) DO UPDATE` keyed on `product_id`, and additionally
  tracks `event_id` in a small dedup table so a redelivered message (Kafka's
  at-least-once guarantee) is a safe no-op, not a duplicate write — the exact
  same idempotency pattern already proven out in
  `week5_resilience/pipeline_week5.py::idempotent_load`.
- **Backfill / replay:** because Kafka retains messages for a configurable
  window (default 7 days, tunable to "forever" with log compaction), you can
  **replay** history by resetting the consumer group's offset:
  ```bash
  kafka-consumer-groups --bootstrap-server localhost:9092 \
      --group warehouse-loader --topic product-cdc-events \
      --reset-offsets --to-earliest --execute
  ```
  This is the streaming equivalent of Week 5's backfill exercise — instead of
  detecting bad rows and re-extracting from source-of-truth, you rewind the
  stream and reprocess it; idempotency is what makes that safe to do live.

## Running it yourself

```bash
cd orchestration/kafka
docker compose up -d              # starts a single-node Kafka broker (KRaft mode, no Zookeeper needed)
pip install kafka-python
python3 producer.py               # streams simulated CDC events from styles.csv
python3 consumer.py               # consumes + idempotently upserts into SQLite
```

Kill and restart `consumer.py` mid-stream — you'll see it pick up from its
last committed offset and **not** duplicate rows in the warehouse, proving the
idempotency claim rather than just asserting it.
