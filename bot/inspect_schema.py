import asyncio
import os
import asyncpg
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("db_inspect")

async def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        return

    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    try:
        conn = await asyncpg.connect(db_url, statement_cache_size=0)
        logger.info("✅ Connected to database.")

        # Get column info for both tables
        for table_name in ["users", "telegram_users"]:
            cols = await conn.fetch(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}' AND table_schema = 'public'
                ORDER BY ordinal_position
            """)
            logger.info(f"Schema for {table_name}:")
            for c in cols:
                logger.info(f"  - {c['column_name']}: {c['data_type']}")

        await conn.close()
    except Exception as e:
        logger.error(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
