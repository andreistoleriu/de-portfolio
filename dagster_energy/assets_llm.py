"""
Dagster assets pentru LLM Observability.
Orchestrează: traffic simulation → dbt models → anomaly check
"""

import subprocess
from dagster import asset, AssetExecutionContext
from pathlib import Path
import duckdb

PROJECT_ROOT  = Path("/home/andrei/de-portfolio")
PYTHON_PATH   = PROJECT_ROOT / "venv/bin/python"
DBT_BINARY    = PROJECT_ROOT / "venv/bin/dbt"
DBT_PATH      = PROJECT_ROOT / "dbt_llm"
PRODUCER_PATH = PROJECT_ROOT / "llm_observability/src/llm_producer_kafka.py"
DUCKDB_PATH   = PROJECT_ROOT / "data/processed/llm_logs.duckdb"


@asset(
    group_name="llm_observability",
    description="Simulează trafic LLM real — trimite prompturi la Llama via Kafka"
)
def llm_traffic(context: AssetExecutionContext):
    context.log.info("🚀 Simulez trafic LLM...")

    result = subprocess.run(
        [str(PYTHON_PATH), str(PRODUCER_PATH)],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT)
    )

    context.log.info(result.stdout)

    if result.returncode != 0:
        raise Exception(f"Producer failed:\n{result.stderr}")

    context.log.info("✅ Trafic generat si trimis in Kafka")


@asset(
    deps=[llm_traffic],
    group_name="llm_observability",
    description="Rulează modele dbt pentru LLM logs"
)
def llm_dbt_models(context: AssetExecutionContext):
    context.log.info("🔧 Rulez modele dbt...")

    result = subprocess.run(
        [str(DBT_BINARY), "run"],
        capture_output=True,
        text=True,
        cwd=str(DBT_PATH)
    )

    context.log.info(result.stdout)

    if result.returncode != 0:
        raise Exception(f"dbt failed:\n{result.stderr}")

    context.log.info("✅ Modele dbt actualizate")


@asset(
    deps=[llm_dbt_models],
    group_name="llm_observability",
    description="Verifică anomalii de cost — alertează dacă costul e anormal"
)
def anomaly_check(context: AssetExecutionContext):
    context.log.info("🔍 Verifică anomalii de cost...")

    conn = duckdb.connect(str(DUCKDB_PATH))

    # cost mediu per request
    avg_cost = conn.execute("""
        SELECT ROUND(AVG(cost_usd), 4)
        FROM staging.stg_llm_logs
    """).fetchone()[0]

    # cel mai scump request din ultima ora
    max_cost = conn.execute("""
        SELECT ROUND(MAX(cost_usd), 4)
        FROM staging.stg_llm_logs
        WHERE request_timestamp >= NOW() - INTERVAL 1 HOUR
    """).fetchone()[0]

    # user cu cel mai mare cost total
    top_user = conn.execute("""
        SELECT user_id, ROUND(SUM(cost_usd), 4) AS total
        FROM staging.stg_llm_logs
        GROUP BY user_id
        ORDER BY total DESC
        LIMIT 1
    """).fetchone()

    conn.close()

    context.log.info(f"💰 Cost mediu per request: ${avg_cost}")
    context.log.info(f"🔥 Cost maxim ultima oră:  ${max_cost}")
    context.log.info(f"👤 Top user: {top_user[0]} — ${top_user[1]}")

    # anomalie dacă max_cost e de 3x mai mare decât media
    if max_cost and avg_cost and max_cost > avg_cost * 3:
        context.log.warning(
            f"⚠️ ANOMALIE DETECTATĂ! "
            f"Max cost ${max_cost} e de {round(max_cost/avg_cost, 1)}x "
            f"mai mare decât media ${avg_cost}"
        )
    else:
        context.log.info("✅ Costuri normale — nicio anomalie")
