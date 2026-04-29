"""
LLM Observability — Etapa 2
Consumer: citeste din Kafka si salveaza in DuckDB.
Ruleaza continuu — proceseaza fiecare mesaj pe masura ce apare.
"""

import json
import duckdb
from datetime import datetime
from pathlib import Path
from kafka import KafkaConsumer

# --- paths ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DUCKDB_PATH  = PROJECT_ROOT / "data" / "processed" / "llm_logs.duckdb"

# --- config ---
KAFKA_TOPIC = "llm_events"
KAFKA_HOST  = "localhost:9092"
GROUP_ID    = "llm_observability_group"


def setup_database():
    conn = duckdb.connect(str(DUCKDB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS llm_logs_kafka (
            timestamp       TIMESTAMP,
            user_id         VARCHAR,
            model           VARCHAR,
            prompt          VARCHAR,
            response        VARCHAR,
            prompt_tokens   INTEGER,
            output_tokens   INTEGER,
            total_tokens    INTEGER,
            latency_ms      INTEGER,
            cost_usd        DOUBLE,
            consumed_at     TIMESTAMP
        )
    """)
    conn.close()
    print("✅ Database ready")


def process_message(message: dict):
    conn = duckdb.connect(str(DUCKDB_PATH))
    conn.execute("""
        INSERT INTO llm_logs_kafka VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, [
        message["timestamp"],
        message["user_id"],
        message["model"],
        message["prompt"],
        message["response"],
        message["prompt_tokens"],
        message["output_tokens"],
        message["total_tokens"],
        message["latency_ms"],
        message["cost_usd"],
        datetime.now().isoformat(),
    ])
    conn.close()
    print(f"  ✅ Salvat: {message['user_id']} | {message['total_tokens']} tokens | ${message['cost_usd']}")


def run():
    print("👂 LLM Observability — Consumer pornit")
    print(f"📨 Ascult pe topic: {KAFKA_TOPIC}")
    print(f"👥 Group ID: {GROUP_ID}")
    print("⏳ Astept mesaje... (Ctrl+C pentru oprire)\n")

    setup_database()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_HOST,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    messages_processed = 0

    try:
        for message in consumer:
            data = message.value
            print(f"\n📨 Mesaj nou din Kafka:")
            print(f"   Partition: {message.partition} | Offset: {message.offset}")
            process_message(data)
            messages_processed += 1
            print(f"   Total procesate: {messages_processed}")

    except KeyboardInterrupt:
        print(f"\n🛑 Consumer oprit.")
        print(f"📊 Total mesaje procesate: {messages_processed}")
        consumer.close()


if __name__ == "__main__":
    run()
