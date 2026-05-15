import asyncio
import os
import asyncpg
import logging
import sys

# Add bot directory to sys.path to import modules
sys.path.append(os.path.join(os.getcwd(), 'bot'))

from utils.names import generate_anonymous_name

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migration")

async def migrate_users():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        logger.error("No DATABASE_URL found.")
        return

    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    try:
        conn = await asyncpg.connect(db_url, statement_cache_size=0)
        logger.info("✅ Connected to database.")

        # Check existing names and IDs
        existing_names = set(await conn.fetchval("SELECT array_agg(anonymous_name) FROM users") or [])
        existing_ids = set(await conn.fetchval("SELECT array_agg(id) FROM users") or [])

        # Get legacy users who are NOT in the new 'users' table
        legacy_users = await conn.fetch("SELECT id FROM telegram_users")
        legacy_users_to_migrate = [u for u in legacy_users if u['id'] not in existing_ids]
        
        logger.info(f"Found {len(legacy_users_to_migrate)} legacy users to migrate.")

        if not legacy_users_to_migrate:
            logger.info("No users to migrate.")
            await conn.close()
            return

        batch_data = []
        for u in legacy_users_to_migrate:
            user_id = u['id']
            name = generate_anonymous_name()
            while name in existing_names:
                name = generate_anonymous_name()
            existing_names.add(name)
            batch_data.append((user_id, name, 'active'))

        # Batch insert into 'users' table
        await conn.executemany("""
            INSERT INTO users (id, anonymous_name, status, joined_at, last_activity_at) 
            VALUES ($1, $2, $3, NOW(), NOW())
            ON CONFLICT (id) DO NOTHING
        """, batch_data)

        logger.info(f"✅ Successfully migrated {len(batch_data)} users to the 'users' table.")
        await conn.close()
    except Exception as e:
        logger.error(f"❌ Migration error: {e}")

if __name__ == "__main__":
    asyncio.run(migrate_users())
