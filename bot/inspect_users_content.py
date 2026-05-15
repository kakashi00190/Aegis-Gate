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

        # Get some users from 'users' table
        users = await conn.fetch("SELECT * FROM users LIMIT 10")
        logger.info("Content of 'users' table:")
        for u in users:
            logger.info(f"  - {dict(u)}")

        await conn.close()
    except Exception as e:
        logger.error(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
