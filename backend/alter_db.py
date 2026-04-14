import asyncio
from sqlalchemy import text
from src.db.session import async_engine

async def alter_db():
    try:
        async with async_engine.begin() as conn:
            # PostgreSQL command to add column safely
            await conn.execute(text("ALTER TABLE job ADD COLUMN search_keyword VARCHAR(100)"))
            await conn.execute(text("CREATE INDEX idx_job_search_keyword ON job (search_keyword)"))
            print("Successfully added search_keyword column")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("Column search_keyword or index already exists, skipping.")
        else:
            print("Failed to alter table:", e)

    await async_engine.dispose()

if __name__ == "__main__":
    asyncio.run(alter_db())
