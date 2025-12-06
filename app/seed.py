import asyncio
import random
import time
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/litellm_costs"
engine = create_async_engine(DATABASE_URL, future=True, echo=False)

PROVIDERS = ["openai", "anthropic", "cohere", "azure", "bedrock"]
MODELS = ["gpt-4o", "claude-3", "command-r", "gpt-3.5-turbo", "sonnet"]

async def seed_cost_logs(total_rows=10_000_000, batch_size=100_000):
    start = time.time()
    inserted = 0
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE cost_logs;"))
        await conn.execute(text("TRUNCATE TABLE spend_summary;"))

    while inserted < total_rows:
        data = [
            {
                "p": random.choice(PROVIDERS),
                "m": random.choice(MODELS),
                "c": round(random.uniform(0.001, 0.05), 6),
            }
            for _ in range(batch_size)
        ]
        async with engine.begin() as conn:
            await conn.execute(
                text("INSERT INTO cost_logs (provider, model, cost_usd) VALUES (:p, :m, :c)"),
                data,
            )
        inserted += batch_size
        elapsed = time.time() - start
        rate = inserted / elapsed
        print(f"Inserted {inserted:,}/{total_rows:,} rows  ({rate:,.0f} rows/sec)")

    print(f"✅ Done. Inserted {inserted:,} rows in {time.time()-start:.2f}s.")

if __name__ == "__main__":
    asyncio.run(seed_cost_logs())
