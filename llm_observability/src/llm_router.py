"""
LLM Router — trimite fiecare prompt la modelul potrivit
bazat pe lungime, complexitate si tip de task.
Simulează cum funcționează un AI Gateway în producție.
"""

import requests
import time
import json
from datetime import datetime
from kafka import KafkaProducer

# --- config ---
OLLAMA_URL  = "http://localhost:11434/api/generate"
KAFKA_TOPIC = "llm_events"
KAFKA_HOST  = "localhost:9092"

# costuri simulate per model (USD per 1000 tokene)
# reflectă prețurile reale ale echivalentelor cloud
MODELS = {
    "phi3": {
        "input_cost":  0.0001,   # echivalent cu Gemini Flash
        "output_cost": 0.0002,
        "strength":    "fast and cheap — good for simple questions",
    },
    "llama3.2": {
        "input_cost":  0.001,    # echivalent cu GPT-3.5
        "output_cost": 0.002,
        "strength":    "balanced — good general purpose model",
    },
    "mistral": {
        "input_cost":  0.002,    # echivalent cu GPT-4o Mini
        "output_cost": 0.006,
        "strength":    "strong reasoning — good for complex and technical tasks",
    },
}

def route_prompt(prompt: str) -> str:
    """
    Decide care model primește prompt-ul.
    Logica reflectă un AI Gateway real din producție.
    """
    words      = prompt.lower().split()
    word_count = len(words)

    # --- regula 1: date sensibile → model local izolat ---
    sensitive_keywords = [
        "password", "secret", "confidential", "parola",
        "ssn", "credit card", "salary", "salariu"
    ]
    if any(kw in prompt.lower() for kw in sensitive_keywords):
        print(f"  🔒 Router: date sensibile detectate → phi3 (local, izolat)")
        return "phi3"

    # --- regula 2: prompt scurt și simplu → phi3 (ieftin, rapid) ---
    if word_count < 10:
        print(f"  ⚡ Router: prompt scurt ({word_count} cuvinte) → phi3")
        return "phi3"

    # --- regula 3: task tehnic sau cod → mistral (cel mai puternic) ---
    technical_keywords = [
        "code", "sql", "python", "error", "debug",
        "function", "query", "script", "bug", "api",
        "kafka", "dbt", "docker", "pipeline"
    ]
    if any(kw in prompt.lower() for kw in technical_keywords):
        print(f"  🔧 Router: task tehnic detectat → mistral")
        return "mistral"

    # --- regula 4: prompt lung și complex → mistral ---
    if word_count > 30:
        print(f"  📝 Router: prompt complex ({word_count} cuvinte) → mistral")
        return "mistral"

    # --- default: llama3.2 pentru orice altceva ---
    print(f"  🦙 Router: prompt general → llama3.2")
    return "llama3.2"

def call_model(prompt: str, model: str, user_id: str) -> dict:
    """
    Trimite prompt-ul la modelul ales și returnează log complet.
    Identic cu llm_producer_kafka.py — doar modelul e variabil.
    """
    config = MODELS[model]
    print(f"  Trimit la {model}...")

    start_time = time.time()

    response = requests.post(
        OLLAMA_URL,
        json={
            "model":  model,
            "prompt": prompt,
            "stream": False,
        },
        timeout=120
    )
    response.raise_for_status()

    latency_ms    = int((time.time() - start_time) * 1000)
    data          = response.json()
    response_text = data["response"]
    prompt_tokens = data.get("prompt_eval_count", 0)
    output_tokens = data.get("eval_count", 0)

    cost_usd = round(
        (prompt_tokens / 1000) * config["input_cost"] +
        (output_tokens / 1000) * config["output_cost"],
        6
    )

    return {
        "timestamp":     datetime.now().isoformat(),
        "user_id":       user_id,
        "model":         model,
        "model_strength":config["strength"],
        "prompt":        prompt,
        "response":      response_text,
        "prompt_tokens": prompt_tokens,
        "output_tokens": output_tokens,
        "total_tokens":  prompt_tokens + output_tokens,
        "latency_ms":    latency_ms,
        "cost_usd":      cost_usd,
        "routed_by":     "llm_router",
    }


def send_to_kafka(producer: KafkaProducer, log: dict):
    """Trimite logul în Kafka."""
    producer.send(
        KAFKA_TOPIC,
        value=log,
        key=log["user_id"].encode("utf-8")
    )
    producer.flush()
    print(f"  ✅ {log['model']} → {log['latency_ms']}ms | "
          f"{log['total_tokens']} tokens | ${log['cost_usd']}")
    
# --- prompturi diverse pentru a testa routing-ul ---
TEST_PROMPTS = [
    # scurte → phi3
    ("user_001", "What is dbt?"),
    ("user_002", "Define Kafka."),
    ("user_003", "What is SQL?"),

    # tehnice → mistral
    ("user_001", "Write a Python function that reads a CSV and loads it into DuckDB"),
    ("user_004", "Debug this SQL query: SELECT * FROM orders WHERE date > '2024-01-01'"),
    ("user_005", "How do I configure a Kafka consumer group in Python?"),

    # generale → llama3.2
    ("user_002", "What are the best practices for data engineering?"),
    ("user_003", "Explain the medallion architecture in simple terms"),
    ("user_006", "What is the difference between a data lake and a data warehouse?"),

    # sensibile → phi3 (privacy)
    ("user_001", "My salary is 5000 EUR, how should I optimize my taxes?"),
]


def run():
    print("🚀 LLM Router — Model Comparison")
    print(f"📊 Models: {list(MODELS.keys())}")
    print(f"📨 Kafka topic: {KAFKA_TOPIC}\n")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_HOST,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        key_serializer=lambda x: x if isinstance(x, bytes) else x.encode("utf-8"),
    )
    print("✅ Conectat la Kafka\n")

    results = []

    for i, (user_id, prompt) in enumerate(TEST_PROMPTS, 1):
        print(f"\n[{i}/{len(TEST_PROMPTS)}] [{user_id}]")
        print(f"  Prompt: {prompt[:60]}...")

        # router decide modelul
        model = route_prompt(prompt)

        # apelam modelul ales
        log = call_model(prompt, model, user_id)
        send_to_kafka(producer, log)
        results.append(log)

    producer.close()

    # summary
    print("\n" + "="*50)
    print("📊 ROUTING SUMMARY")
    print("="*50)

    for model in MODELS:
        model_logs = [r for r in results if r["model"] == model]
        if model_logs:
            avg_cost    = round(sum(r["cost_usd"] for r in model_logs) / len(model_logs), 6)
            avg_latency = round(sum(r["latency_ms"] for r in model_logs) / len(model_logs))
            print(f"\n{model}:")
            print(f"  Requests:    {len(model_logs)}")
            print(f"  Avg cost:    ${avg_cost}")
            print(f"  Avg latency: {avg_latency}ms")

    print(f"\n✅ Total: {len(results)} prompts routed și trimise în Kafka")


if __name__ == "__main__":
    run()