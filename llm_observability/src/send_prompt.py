import requests
import time
import json
from datetime import datetime
from kafka import KafkaProducer

OLLAMA_URL  = "http://localhost:11434/api/generate"
MODEL       = "llama3.2"
KAFKA_TOPIC = "llm_events"
KAFKA_HOST  = "localhost:9092"
COST_PER_1K_INPUT  = 0.01
COST_PER_1K_OUTPUT = 0.03

producer = KafkaProducer(
    bootstrap_servers=KAFKA_HOST,
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)

print("🤖 LLM Observability — Interactive Mode")
print("Scrie 'exit' pentru a iesi\n")

user_id = input("User ID (ex: user_009): ").strip() or "user_009"

while True:
    prompt = input(f"\n[{user_id}] Intrebare: ").strip()

    if prompt.lower() == "exit":
        print("La revedere!")
        break

    if not prompt:
        continue

    print("Trimit la Llama...")
    start = time.time()

    response = requests.post(
        OLLAMA_URL,
        json={"model": MODEL, "prompt": prompt, "stream": False},
        timeout=60
    )
    data = response.json()
    latency_ms = int((time.time() - start) * 1000)

    prompt_tokens = data.get("prompt_eval_count", 0)
    output_tokens = data.get("eval_count", 0)
    cost = round(
        (prompt_tokens / 1000) * COST_PER_1K_INPUT +
        (output_tokens / 1000) * COST_PER_1K_OUTPUT,
        6
    )

    print(f"\nRaspuns:\n{data['response']}")
    print(f"\n{latency_ms}ms | {prompt_tokens+output_tokens} tokens | ${cost}")

    log = {
        "timestamp":     datetime.now().isoformat(),
        "user_id":       user_id,
        "model":         MODEL,
        "prompt":        prompt,
        "response":      data["response"],
        "prompt_tokens": prompt_tokens,
        "output_tokens": output_tokens,
        "total_tokens":  prompt_tokens + output_tokens,
        "latency_ms":    latency_ms,
        "cost_usd":      cost,
    }

    producer.send(KAFKA_TOPIC, value=log)
    producer.flush()
    print("Logat in Kafka → DuckDB")
