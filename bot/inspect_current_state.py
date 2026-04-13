
import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

async def inspect():
    database_url = os.getenv("DATABASE_URL")
    pool = await asyncpg.create_pool(database_url)
    
    async with pool.acquire() as conn:
        print("--- Sessions ---")
        sessions = await conn.fetch("SELECT * FROM sessions ORDER BY id DESC LIMIT 5")
        for s in sessions:
            print(dict(s))
            
        print("\n--- Leaderboard (session_upload_count > 0) ---")
        leaders = await conn.fetch(
            "SELECT anonymous_name, session_upload_count FROM users WHERE session_upload_count > 0 ORDER BY session_upload_count DESC LIMIT 5"
        )
        for l in leaders:
            print(dict(l))
            
        print("\n--- Top Users by Badge ---")
        badges = await conn.fetch(
            "SELECT anonymous_name, badge_emoji, session_upload_count FROM users WHERE badge_emoji != '' ORDER BY session_upload_count DESC LIMIT 5"
        )
        for b in badges:
            print(dict(b))
            
    await pool.close()

if __name__ == "__main__":
    asyncio.run(inspect())
