import asyncio
import time
from datetime import datetime
from typing import Dict, Tuple, List
from sqlalchemy.ext.asyncio import create_async_engine, AsyncConnection
from sqlalchemy import text

# ===============================================================
# 🚀 LiteLLM Spend Aggregator — Parallel + Optimized
# ===============================================================

DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/litellm_costs"
engine = create_async_engine(DATABASE_URL, future=True, echo=False)

KNOWN_PROVIDERS: List[str] = []  # e.g. ["openai", "anthropic", "cohere", "azure", "bedrock"]

# ---------------------------------------------------------------
# 🧱 Initialize Schema + Indexes
# ---------------------------------------------------------------
async def init_db():
    async with engine.begin() as conn:
        await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS cost_logs (
            id BIGSERIAL PRIMARY KEY,
            provider TEXT NOT NULL,
            model TEXT NOT NULL,
            cost_usd FLOAT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """))

        await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS spend_summary (
            provider TEXT NOT NULL,
            model TEXT NOT NULL,
            total_cost FLOAT DEFAULT 0,
            updated_at TIMESTAMP DEFAULT NOW(),
            PRIMARY KEY (provider, model)
        );
        """))

        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_cost_logs_provider ON cost_logs(provider);"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_cost_logs_provider_model ON cost_logs(provider, model);"))

    print("✅ Tables & indexes ready.")


# ---------------------------------------------------------------
# 🔩 Per-provider Worker
# ---------------------------------------------------------------
async def _aggregate_provider(conn: AsyncConnection, provider: str, batch_limit: int | None = None
) -> Tuple[Dict[Tuple[str, str], float], int]:
    agg: Dict[Tuple[str, str], float] = {}
    total_rows = 0

    query = text("""
        SELECT provider, model, cost_usd
        FROM cost_logs
        WHERE provider = :p
        """ + (f"LIMIT {batch_limit}" if batch_limit else ""))
    result = await conn.stream(query, {"p": provider})

    async for row in result:
        key = (row.provider, row.model)
        agg[key] = agg.get(key, 0.0) + float(row.cost_usd)
        total_rows += 1

    return agg, total_rows


# ---------------------------------------------------------------
# 🧮 Parallel Aggregation
# ---------------------------------------------------------------
async def aggregate_incremental_parallel(max_workers: int = 8, batch_limit: int | None = None):
    t0 = time.time()

    async with engine.connect() as conn:
        if KNOWN_PROVIDERS:
            providers = KNOWN_PROVIDERS
        else:
            res = await conn.execute(text("SELECT DISTINCT provider FROM cost_logs;"))
            providers = [r[0] for r in res.fetchall()]

        pending = await conn.execute(text("SELECT COUNT(*) FROM cost_logs;"))
        pending_rows = pending.scalar_one()

    if pending_rows == 0 or len(providers) == 0:
        print("⏸ No new LiteLLM logs to aggregate.")
        return 0

    print(f"\n🧩 Found {pending_rows:,} logs across {len(providers)} providers — aggregating in parallel...")

    sem = asyncio.Semaphore(max_workers)

    async def shard_task(provider: str):
        async with sem:
            async with engine.connect() as conn:
                return await _aggregate_provider(conn, provider, batch_limit)

    shard_start = time.time()
    results = await asyncio.gather(*[shard_task(p) for p in providers])
    shard_duration = time.time() - shard_start

    merged: Dict[Tuple[str, str], float] = {}
    scanned_rows = 0
    for part_agg, part_rows in results:
        scanned_rows += part_rows
        for key, val in part_agg.items():
            merged[key] = merged.get(key, 0.0) + val

    async with engine.begin() as conn:
        if merged:
            values = [
                {"p": k[0], "m": k[1], "c": v, "t": datetime.utcnow()}
                for k, v in merged.items()
            ]
            await conn.execute(
                text("""
                INSERT INTO spend_summary (provider, model, total_cost, updated_at)
                VALUES (:p, :m, :c, :t)
                ON CONFLICT (provider, model)
                DO UPDATE SET
                    total_cost = spend_summary.total_cost + EXCLUDED.total_cost,
                    updated_at = :t;
                """),
                values,
            )
            await conn.execute(text("TRUNCATE TABLE cost_logs;"))

    total_duration = time.time() - t0
    print(f"✅ Aggregation done in {total_duration:.2f}s "
          f"(scan {shard_duration:.2f}s, {len(merged)} groups, {scanned_rows:,} logs)")
    return scanned_rows


# ---------------------------------------------------------------
# ♻️ Background Loop Wrapper
# ---------------------------------------------------------------
async def run_forever(interval: int = 30, max_workers: int = 8, batch_limit: int | None = None):
    while True:
        try:
            await aggregate_incremental_parallel(max_workers, batch_limit)
        except Exception as e:
            print(f"⚠️ Aggregation error: {e}")
        await asyncio.sleep(interval)
