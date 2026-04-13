import asyncio
import os
import sys
import socket
import logging
import asyncpg
from aiogram import Bot
from aiogram.exceptions import TelegramUnauthorizedError

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("validator")

async def validate_telegram_token(token: str):
    logger.info("Validating Telegram Bot Token...")
    bot = Bot(token=token)
    try:
        user = await bot.get_me()
        logger.info(f"✅ Bot token is valid. Bot: @{user.username} (ID: {user.id})")
        return True
    except TelegramUnauthorizedError:
        logger.error("❌ Invalid Telegram Bot Token.")
        return False
    except Exception as e:
        logger.error(f"❌ Error validating bot token: {e}")
        return False
    finally:
        await bot.session.close()

async def validate_database(url: str):
    logger.info("Validating Database Connection...")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    
    # Try multiple times to handle transient Supabase network issues
    for attempt in range(1, 4):
        try:
            # Just check connectivity with a simple SELECT 1
            # No need to check tables here as init_db handles that
            conn = await asyncpg.connect(url, timeout=15, statement_cache_size=0)
            await conn.execute("SELECT 1")
            await conn.close()
            logger.info(f"✅ Database connection successful on attempt {attempt}.")
            return True
        except Exception as e:
            if attempt < 3:
                logger.warning(f"⚠️ Database connection attempt {attempt} failed: {e}. Retrying in 5s...")
                await asyncio.sleep(5)
            else:
                logger.error(f"❌ Database connection failed after {attempt} attempts: {e}")
                return False

def validate_admin_id(admin_id_str: str):
    logger.info("Validating Admin ID...")
    try:
        # Support both single ID and comma-separated IDs
        ids = [int(i.strip()) for i in admin_id_str.split(',')]
        logger.info(f"✅ Admin ID(s) valid: {ids}")
        return True
    except ValueError:
        logger.error(f"❌ Invalid Admin ID format: {admin_id_str}. Must be an integer or comma-separated integers.")
        return False

async def main():
    # Required Environment Variables
    token = os.environ.get("BOT_TOKEN")
    admin_id = os.environ.get("ADMIN_ID")
    db_url = os.environ.get("DATABASE_URL")
    port_str = os.environ.get("PORT", "8080")

    if not all([token, admin_id, db_url]):
        missing = []
        if not token: missing.append("BOT_TOKEN")
        if not admin_id: missing.append("ADMIN_ID")
        if not db_url: missing.append("DATABASE_URL")
        logger.error(f"❌ Missing required environment variables: {', '.join(missing)}")
        return False

    try:
        port = int(port_str)
    except ValueError:
        logger.error(f"❌ Invalid PORT value: {port_str}")
        return False

    # Run checks
    success = True
    
    if not await validate_telegram_token(token):
        success = False
    
    if not await validate_database(db_url):
        success = False
    
    if not validate_admin_id(admin_id):
        success = False
    
    # Optional: Port check might fail if the port is already bound by the health server in a previous run
    # but for a fresh startup it should be fine.
    # if not validate_port(port):
    #     success = False

    if success:
        logger.info("🚀 All checks passed! Starting bot...")
        return True
    else:
        logger.error("🛑 Environment validation failed. Fix the issues and try again.")
        return False

if __name__ == "__main__":
    if not asyncio.run(main()):
        sys.exit(1)
    sys.exit(0)
