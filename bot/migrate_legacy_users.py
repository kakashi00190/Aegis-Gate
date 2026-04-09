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

        # Check existing names to avoid collisions
        existing_names = set(await conn.fetchval("SELECT array_agg(anonymous_name) FROM users"))
        if not existing_names or existing_names == {None}:
            existing_names = set()

        # Get legacy users who are NOT in the new 'users' table
        legacy_users = await conn.fetch("""
            SELECT id FROM telegram_users 
            WHERE id NOT IN (SELECT id FROM users)
        """)
        
        logger.info(f"Found {len(legacy_users)} legacy users to migrate.")

        if not legacy_users:
            logger.info("No users to migrate.")
            await conn.close()
            return

        migrated_count = 0
        for u in legacy_users:
            user_id = u['id']
            
            # Generate a unique name
            name = generate_anonymous_name()
            while name in existing_names:
                name = generate_anonymous_name()
            
            existing_names.add(name)
            
            # Insert into 'users' table
            # Setting status to 'active' since they were active in the legacy system
            await conn.execute("""
                INSERT INTO users (id, anonymous_name, status, joined_at, last_activity_at) 
                VALUES ($1, $2, 'active', NOW(), NOW())
                ON CONFLICT (id) DO NOTHING
            """, user_id, name)
            
            migrated_count += 1
            if migrated_count % 100 == 0:
                logger.info(f"Migrated {migrated_count} users...")

        logger.info(f"✅ Successfully migrated {migrated_count} users to the 'users' table.")
        await conn.close()
    except Exception as e:
        logger.error(f"❌ Migration error: {e}")

if __name__ == "__main__":
    asyncio.run(migrate_users())
