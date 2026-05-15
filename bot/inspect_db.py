import asyncio
import os
import asyncpg
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("db_inspect")

async def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        logger.error("No DATABASE_URL found.")
        return

    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    try:
        conn = await asyncpg.connect(db_url, statement_cache_size=0)
        logger.info("✅ Connected to database.")

        # Check search_path
        search_path = await conn.fetchval("SHOW search_path")
        logger.info(f"Current search_path: {search_path}")

        # List all tables in all schemas
        tables = await conn.fetch("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name
        """)
        
        logger.info("Tables found:")
        for t in tables:
            schema = t['table_schema']
            name = t['table_name']
            count = await conn.fetchval(f'SELECT COUNT(*) FROM "{schema}"."{name}"')
            logger.info(f"  - {schema}.{name}: {count} rows")

        await conn.close()
    except Exception as e:
        logger.error(f"❌ Error inspecting database: {e}")

if __name__ == "__main__":
    asyncio.run(main())
