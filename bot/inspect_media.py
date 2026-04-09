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

        # List all tables starting with 'media' or containing 'message' or 'broadcast'
        tables = await conn.fetch("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
              AND (table_name LIKE 'media%' OR table_name LIKE '%message%' OR table_name LIKE '%broadcast%')
            ORDER BY table_name
        """)
        
        for t in tables:
            name = t['table_name']
            cols = await conn.fetch(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{name}' AND table_schema = 'public'
                ORDER BY ordinal_position
            """)
            logger.info(f"Schema for {name}:")
            for c in cols:
                logger.info(f"  - {c['column_name']}: {c['data_type']}")
            
            count = await conn.fetchval(f'SELECT COUNT(*) FROM "public"."{name}"')
            logger.info(f"  - Total: {count} rows")

        await conn.close()
    except Exception as e:
        logger.error(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
