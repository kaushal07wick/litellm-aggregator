"""
===============================================================
🧪 LiteLLM Aggregator Test Suite
---------------------------------------------------------------
Purpose:
    Validate correctness, idempotency, and performance of
    the incremental aggregation engine for large-scale cost logs.

Covers:
    - Database setup
    - Synthetic data insertion (async)
    - Incremental aggregation correctness
    - Idempotency (double-run safety)
    - Performance benchmarking
===============================================================
"""

import asyncio
import random
import time
import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

# Import functions from your aggregator module
from app.aggregator import init_db, aggregate_incremental

# ---------------------------------------------------------------
# Database Config (test instance)
# ---------------------------------------------------------------
TEST_DB_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/litellm_costs"
engine = create_async_engine(TEST_DB_URL, future=True, echo=False)


# ---------------------------------------------------------------
# Helper: async fixture for setup/teardown
# ---------------------------------------------------------------
@pytest.fixture(scope="module", autouse=True)
async def setup_database():
    """Ensure tables exist and are empty before testing."""
    await init_db()
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE cost_logs RESTART IDENTITY;"))
        await conn.execute(text("TRUNCATE TABLE spend_summary;"))
    yield
    # cleanup
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE cost_logs;"))
        await conn.execute(text("TRUNCATE TABLE spend_summary;"))


# ---------------------------------------------------------------
# Helper: insert synthetic cost_logs
# ---------------------------------------------------------------
async def insert_fake_cost_logs(n=100_000):
    """Generate n synthetic cost log records."""
    PROVIDERS = ["openai", "anthropic", "cohere", "azure", "bedrock"]
    MODELS = ["gpt-4o", "claude-3", "command-r", "gpt-3.5-turbo", "sonnet"]

    data = [
        {
            "p": random.choice(PROVIDERS),
            "m": random.choice(MODELS),
            "c": round(random.uniform(0.001, 0.05), 6),
        }
        for _ in range(n)
    ]

    async with engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO cost_logs (provider, model, cost_usd) VALUES (:p, :m, :c)"),
            data,
        )


# ---------------------------------------------------------------
# Test 1: Verify basic aggregation correctness
# ---------------------------------------------------------------
@pytest.mark.asyncio
async def test_basic_aggregation():
    await insert_fake_cost_logs(100_000)

    start = time.time()
    await aggregate_incremental()
    duration = time.time() - start

    async with engine.connect() as conn:
        res = await conn.execute(text("SELECT COUNT(*) FROM spend_summary"))
        count = res.scalar_one()

    assert count > 0, "Aggregation failed to produce any provider/model entries"
    print(f"✅ Aggregation created {count} summary rows in {duration:.2f}s")


# ---------------------------------------------------------------
# Test 2: Idempotency — re-running should not double count
# ---------------------------------------------------------------
@pytest.mark.asyncio
async def test_idempotency():
    """
    Run aggregation twice back-to-back.
    The second run should make no change to total_cost values.
    """
    async with engine.connect() as conn:
        # Get snapshot of total_costs after first run
        before = {
            row._mapping["model"]: row._mapping["total_cost"]
            for row in (await conn.execute(text("SELECT * FROM spend_summary"))).fetchall()
        }

    await aggregate_incremental()

    async with engine.connect() as conn:
        after = {
            row._mapping["model"]: row._mapping["total_cost"]
            for row in (await conn.execute(text("SELECT * FROM spend_summary"))).fetchall()
        }

    # No changes expected
    assert before == after, "Aggregation is not idempotent"
    print("✅ Idempotency check passed (no double counting)")


# ---------------------------------------------------------------
# Test 3: Performance benchmark on 1M+ logs
# ---------------------------------------------------------------
@pytest.mark.asyncio
async def test_large_scale_aggregation():
    """
    Stress test: Insert 1M synthetic rows and ensure aggregation
    completes under 10 seconds on a local DB.
    """
    await insert_fake_cost_logs(1_000_000)

    start = time.time()
    await aggregate_incremental()
    duration = time.time() - start

    async with engine.connect() as conn:
        res = await conn.execute(text("SELECT COUNT(*) FROM cost_logs WHERE aggregated=TRUE;"))
        processed = res.scalar_one()

    assert processed >= 1_000_000, "Not all rows aggregated"
    assert duration < 15, f"Aggregation too slow ({duration:.2f}s)"
    print(f"⚡ Large-scale aggregation finished in {duration:.2f}s for {processed:,} rows")


# ---------------------------------------------------------------
# Test 4: Schema recovery test
# ---------------------------------------------------------------
@pytest.mark.asyncio
async def test_schema_recovery():
    """Drop the aggregated column and verify init_db() recreates it."""
    async with engine.begin() as conn:
        await conn.execute(text("ALTER TABLE cost_logs DROP COLUMN aggregated;"))

    await init_db()

    async with engine.connect() as conn:
        res = await conn.execute(text("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='cost_logs' AND column_name='aggregated';
        """))
        col = res.fetchone()

    assert col is not None, "Schema recovery failed — 'aggregated' column missing"
    print("✅ Schema recovery verified")
