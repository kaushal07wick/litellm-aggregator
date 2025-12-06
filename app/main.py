import asyncio
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Response
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

from app.aggregator import init_db, aggregate_incremental_parallel  # ✅ fixed import

# ===============================================================
# 🚀 LiteLLM Spend Aggregator + Prometheus Metrics (Parallel)
# ===============================================================

DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/litellm_costs"
engine = create_async_engine(DATABASE_URL, future=True, echo=False)

# ------------------ Prometheus Metrics -------------------------
aggregation_cycles_total = Counter("litellm_aggregation_cycles_total", "Total aggregation cycles")
logs_processed = Counter("litellm_logs_processed_total", "Total LiteLLM log rows processed")
pending_logs_gauge = Gauge("litellm_pending_logs", "Pending LiteLLM cost_logs")
aggregation_duration = Histogram(
    "litellm_aggregation_duration_seconds",
    "Duration of aggregation cycles (seconds)",
    buckets=[0.1, 0.5, 1, 2, 5, 10, 20, 60, 120, 300],
)


# ---------------------------------------------------------------
# ♻️ Aggregation Worker with Prometheus Metrics
# ---------------------------------------------------------------
async def run_forever(interval=30, max_workers=8):
    while True:
        try:
            start = time.time()
            async with engine.connect() as conn:
                pending = await conn.execute(text("SELECT COUNT(*) FROM cost_logs;"))
                pending_rows = pending.scalar_one()
                pending_logs_gauge.set(pending_rows)

            if pending_rows == 0:
                print("⏸ No new LiteLLM logs to aggregate.")
            else:
                print(f"\n🧩 Found {pending_rows:,} new logs — aggregating...")
                with aggregation_duration.time():
                    processed = await aggregate_incremental_parallel(max_workers=max_workers)

                aggregation_cycles_total.inc()
                logs_processed.inc(processed)

                async with engine.connect() as conn:
                    res = await conn.execute(text("""
                        SELECT provider, model, total_cost
                        FROM spend_summary
                        ORDER BY total_cost DESC
                        LIMIT 5;
                    """))
                    top_entries = [dict(r._mapping) for r in res]

                print(f"✅ Aggregated {processed:,} logs | Top models:")
                for e in top_entries:
                    print(f"   • {e['provider']:10} | {e['model']:12} | ${e['total_cost']:.2f}")

            total_time = time.time() - start
            print(f"🕓 Cycle complete in {total_time:.2f}s. Sleeping {interval}s...\n")

        except Exception as e:
            print(f"⚠️ Aggregation error: {e}")

        await asyncio.sleep(interval)


# ---------------------------------------------------------------
# 🧱 FastAPI Lifecycle
# ---------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    task = asyncio.create_task(run_forever(interval=20, max_workers=8))
    print("🚀 Parallel Aggregator started with Prometheus metrics.")
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        print("🧹 Aggregator stopped cleanly.")


# ---------------------------------------------------------------
# ⚡ FastAPI App Definition
# ---------------------------------------------------------------
app = FastAPI(title="LiteLLM Spend Aggregator", description="Parallel async aggregator with Prometheus", lifespan=lifespan)


# ---------------------------------------------------------------
# 🧭 API Endpoints
# ---------------------------------------------------------------
@app.get("/")
async def health():
    return {"status": "ok", "message": "LiteLLM parallel aggregator running."}


@app.get("/aggregate")
async def get_summary():
    async with engine.connect() as conn:
        res = await conn.execute(text("""
            SELECT provider, model, total_cost, updated_at
            FROM spend_summary
            ORDER BY total_cost DESC;
        """))
        rows = [dict(r._mapping) for r in res]
        return {"records": len(rows), "data": rows}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
