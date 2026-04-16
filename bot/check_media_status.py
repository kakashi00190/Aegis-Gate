
import asyncio
import asyncpg
import os
import sys
from dotenv import load_dotenv

# Add the bot directory to path to import config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
load_dotenv()
import config

async def check_db():
    try:
        pool = await asyncpg.create_pool(config.DATABASE_URL)
        async with pool.acquire() as conn:
            active_count = await conn.fetchval("SELECT COUNT(*) FROM users WHERE status = 'active' AND bot_blocked = FALSE")
            inactive_count = await conn.fetchval("SELECT COUNT(*) FROM users WHERE status = 'inactive' AND bot_blocked = FALSE")
            pending_count = await conn.fetchval("SELECT COUNT(*) FROM users WHERE status = 'pending' AND bot_blocked = FALSE")
            banned_count = await conn.fetchval("SELECT COUNT(*) FROM users WHERE status = 'banned'")
            blocked_bot_count = await conn.fetchval("SELECT COUNT(*) FROM users WHERE bot_blocked = TRUE")
            
            queue_count = await conn.fetchval("SELECT COUNT(*) FROM media WHERE sent_at IS NULL AND scheduled_at <= NOW()")
            total_media = await conn.fetchval("SELECT COUNT(*) FROM media")
            
            print(f"--- User Stats ---")
            print(f"Active: {active_count}")
            print(f"Inactive: {inactive_count}")
            print(f"Pending: {pending_count}")
            print(f"Banned: {banned_count}")
            print(f"Blocked bot: {blocked_bot_count}")
            
            print(f"\n--- Media Stats ---")
            print(f"In Queue (due): {queue_count}")
            print(f"Total Media: {total_media}")
            
            if queue_count > 0:
                oldest = await conn.fetchrow("SELECT scheduled_at FROM media WHERE sent_at IS NULL ORDER BY scheduled_at ASC LIMIT 1")
                print(f"Oldest item in queue was scheduled at: {oldest['scheduled_at']}")
            
    except Exception as e:
        print(f"Error: {repr(e)}")
    finally:
        if 'pool' in locals():
            await pool.close()

if __name__ == "__main__":
    asyncio.run(check_db())
