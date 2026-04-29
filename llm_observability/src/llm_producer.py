"""
LLM Observability — Etapa 1
Trimite un prompt la Llama 3.2 si salveaza logul in DuckDB.
"""

import requests
import duckdb
import time
from datetime import datetime
from pathlib import Path

# --- paths ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DUCKDB_PATH  = PROJECT_ROOT / "data" / "processed" / "llm_logs.duckdb"

# --- config ---
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL      = "llama3.2"

# Costuri simulate OpenAI GPT-4 (USD per 1000 tokene)
COST_PER_1K_INPUT_TOKENS  = 0.01
COST_PER_1K_OUTPUT_TOKENS = 0.03

def call_llama(prompt: str, user_id: str) -> dict:
    """
    Trimite un prompt la Llama 3.2 si returneaza un log complet.
    """
    print(f"  [{user_id}] Trimit: {prompt[:50]}...")

    # --- apelul API ---
    start_time = time.time()

    response = requests.post(
        OLLAMA_URL,
        json={
            "model":  MODEL,
            "prompt": prompt,
            "stream": False,  # vrem raspunsul complet, nu streaming
        },
        timeout=60
    )
    response.raise_for_status()

    latency_ms = int((time.time() - start_time) * 1000)

    # --- parsam raspunsul ---
    data           = response.json()
    response_text  = data["response"]
    prompt_tokens  = data.get("prompt_eval_count", 0)
    output_tokens  = data.get("eval_count", 0)

    # --- calculam costul ---
    cost_usd = (
        (prompt_tokens  / 1000) * COST_PER_1K_INPUT_TOKENS +
        (output_tokens  / 1000) * COST_PER_1K_OUTPUT_TOKENS
    )

    log = {
        "timestamp":       datetime.now().isoformat(),
        "user_id":         user_id,
        "model":           MODEL,
        "prompt":          prompt,
        "response":        response_text,
        "prompt_tokens":   prompt_tokens,
        "output_tokens":   output_tokens,
        "total_tokens":    prompt_tokens + output_tokens,
        "latency_ms":      latency_ms,
        "cost_usd":        round(cost_usd, 6),
    }

    print(f"  [{user_id}] Done — {latency_ms}ms, {log['total_tokens']} tokens, ${log['cost_usd']}")
    return log

def setup_database():
    """
    Creaza tabela de loguri daca nu exista.
    Rulează o singură dată la pornirea scriptului.
    """
    conn = duckdb.connect(str(DUCKDB_PATH))

    conn.execute("""
        CREATE TABLE IF NOT EXISTS llm_logs (
            timestamp       TIMESTAMP,
            user_id         VARCHAR,
            model           VARCHAR,
            prompt          VARCHAR,
            response        VARCHAR,
            prompt_tokens   INTEGER,
            output_tokens   INTEGER,
            total_tokens    INTEGER,
            latency_ms      INTEGER,
            cost_usd        DOUBLE
        )
    """)

    conn.close()
    print("✅ Database ready")


def save_log(log: dict):
    """
    Salveaza un singur log in DuckDB.
    """
    conn = duckdb.connect(str(DUCKDB_PATH))

    conn.execute("""
        INSERT INTO llm_logs VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, [
        log["timestamp"],
        log["user_id"],
        log["model"],
        log["prompt"],
        log["response"],
        log["prompt_tokens"],
        log["output_tokens"],
        log["total_tokens"],
        log["latency_ms"],
        log["cost_usd"],
    ])

    conn.close()

    # --- prompturi simulate ---
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
    """
    Rulează toate prompturile și salvează logurile în DuckDB.
    """
    print("🚀 Starting LLM Observability — Etapa 1")
    print(f"📦 Database: {DUCKDB_PATH}")
    print(f"🦙 Model: {MODEL}\n")

    # setup baza de date
    setup_database()

    # rulam fiecare prompt
    for i, (user_id, prompt) in enumerate(USERS_AND_PROMPTS, 1):
        print(f"\n[{i}/{len(USERS_AND_PROMPTS)}]")
        log = call_llama(prompt, user_id)
        save_log(log)

    # verificare finala
    conn = duckdb.connect(str(DUCKDB_PATH))
    count = conn.execute("SELECT COUNT(*) FROM llm_logs").fetchone()[0]
    total_cost = conn.execute("SELECT ROUND(SUM(cost_usd), 4) FROM llm_logs").fetchone()[0]
    avg_latency = conn.execute("SELECT ROUND(AVG(latency_ms), 0) FROM llm_logs").fetchone()[0]
    conn.close()

    print(f"\n✅ Done!")
    print(f"📊 Loguri salvate:  {count}")
    print(f"💰 Cost total:      ${total_cost}")
    print(f"⚡ Latență medie:   {avg_latency}ms")


if __name__ == "__main__":
    run()