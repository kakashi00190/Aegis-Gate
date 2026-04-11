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
    
    try:
        conn = await asyncpg.connect(url, timeout=10)
        logger.info("✅ Database connection successful.")
        
        # Check if tables exist
        tables = ['users', 'sessions', 'media', 'sent_messages', 'admin_config']
        for table in tables:
            exists = await conn.fetchval(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)",
                table
            )
            if not exists:
                logger.warning(f"⚠️ Table '{table}' does not exist yet. It will be created on first run.")
        
        await conn.close()
        return True
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
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
        sys.exit(1)

    try:
        port = int(port_str)
    except ValueError:
        logger.error(f"❌ Invalid PORT value: {port_str}")
        sys.exit(1)

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
        sys.exit(0)
    else:
        logger.error("🛑 Environment validation failed. Fix the issues and try again.")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
