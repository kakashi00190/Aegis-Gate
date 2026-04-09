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
        await conn.close()
        return True
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return False

def validate_port(port: int):
    logger.info(f"Validating Port {port} availability...")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("0.0.0.0", port))
            logger.info(f"✅ Port {port} is available.")
            return True
        except OSError:
            logger.error(f"❌ Port {port} is already in use or blocked.")
            return False

async def main():
    # Required Environment Variables (Mapping User's prompt to bot's config names)
    token = os.environ.get("TELEGRAM_BOT_TOKEN") or os.environ.get("BOT_TOKEN")
    admin_id = os.environ.get("ADMIN_USER_IDS") or os.environ.get("ADMIN_ID")
    db_url = os.environ.get("DATABASE_URL")
    port_str = os.environ.get("PORT", "3000")

    if not all([token, admin_id, db_url]):
        logger.error("❌ Missing required environment variables (TELEGRAM_BOT_TOKEN, ADMIN_USER_IDS, DATABASE_URL).")
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
    
    if not validate_port(port):
        success = False

    if success:
        logger.info("🚀 All checks passed! Starting bot...")
        sys.exit(0)
    else:
        logger.error("🛑 Environment validation failed. Fix the issues and try again.")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
