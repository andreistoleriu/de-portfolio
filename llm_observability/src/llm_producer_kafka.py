"""
LLM Observability — Etapa 2
Producer: trimite logurile in Kafka in loc de DuckDB direct.
Acelasi call_llama() ca inainte — doar destinatia e diferita.
"""

import requests
import time
import json
from datetime import datetime
from kafka import KafkaProducer

# --- config ---
OLLAMA_URL  = "http://localhost:11434/api/generate"
MODEL       = "llama3.2"
KAFKA_TOPIC = "llm_events"
KAFKA_HOST  = "localhost:9092"

COST_PER_1K_INPUT_TOKENS  = 0.01
COST_PER_1K_OUTPUT_TOKENS = 0.03

def call_llama(prompt: str, user_id: str) -> dict:
    """
    Trimite prompt la Llama si returneaza log complet.
    """
    print(f"  [{user_id}] Trimit: {prompt[:50]}...")

    start_time = time.time()

    response = requests.post(
        OLLAMA_URL,
        json={
            "model":  MODEL,
            "prompt": prompt,
            "stream": False,
        },
        timeout=60
    )
    response.raise_for_status()

    latency_ms = int((time.time() - start_time) * 1000)
    data           = response.json()
    response_text  = data["response"]
    prompt_tokens  = data.get("prompt_eval_count", 0)
    output_tokens  = data.get("eval_count", 0)

    cost_usd = (
        (prompt_tokens  / 1000) * COST_PER_1K_INPUT_TOKENS +
        (output_tokens  / 1000) * COST_PER_1K_OUTPUT_TOKENS
    )

    return {
        "timestamp":     datetime.now().isoformat(),
        "user_id":       user_id,
        "model":         MODEL,
        "prompt":        prompt,
        "response":      response_text,
        "prompt_tokens": prompt_tokens,
        "output_tokens": output_tokens,
        "total_tokens":  prompt_tokens + output_tokens,
        "latency_ms":    latency_ms,
        "cost_usd":      round(cost_usd, 6),
    }


def send_to_kafka(producer: KafkaProducer, log: dict):
    """
    Trimite un log in Kafka topic.
    """
    producer.send(
        KAFKA_TOPIC,
        value=log,        # dictionarul nostru
        key=log["user_id"].encode("utf-8")  # cheia = user_id
    )
    producer.flush()  # asigura ca mesajul a ajuns in Kafka
    print(f"  → Trimis in Kafka: {log['user_id']} ${log['cost_usd']}")

USERS_AND_PROMPTS = [
    ("user_001", "What is the difference between a data lake and a data warehouse?"),
    ("user_002", "Explain ETL pipeline in simple terms"),
    ("user_003", "What is Apache Kafka used for?"),
    ("user_004", "How does dbt transform data?"),
    ("user_005", "What are the best practices for data quality?"),
    ("user_001", "What is DuckDB and why is it popular?"),
    ("user_003", "Explain the difference between batch and streaming data processing"),
    ("user_006", "What is a data mesh architecture?"),
    ("user_002", "How do you handle missing data in a pipeline?"),
    ("user_005", "What is the medallion architecture in data engineering?"),
]


def run():
    print("🚀 LLM Observability — Etapa 2 (Kafka Producer)")
    print(f"📨 Kafka topic: {KAFKA_TOPIC}")
    print(f"🦙 Model: {MODEL}\n")

    # --- conectare Kafka ---
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_HOST,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        key_serializer=lambda x: x if isinstance(x, bytes) else x.encode("utf-8"),
    )
    print("✅ Conectat la Kafka\n")

    # --- rulam prompturile ---
    for i, (user_id, prompt) in enumerate(USERS_AND_PROMPTS, 1):
        print(f"\n[{i}/{len(USERS_AND_PROMPTS)}]")
        log = call_llama(prompt, user_id)
        send_to_kafka(producer, log)

    producer.close()
    print(f"\n✅ Toate mesajele trimise in Kafka!")
    print(f"👉 Acum ruleaza consumer-ul pentru a le salva in DuckDB")


if __name__ == "__main__":
    run()