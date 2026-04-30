import asyncio
import logging
from datetime import datetime, timedelta, timezone
from aiogram import Bot
from aiogram.exceptions import TelegramForbiddenError

import asyncpg
from database import get_config, get_inactive_users, mark_user_blocked, is_session_paused

from utils.helpers import format_timedelta_until, safe_error
from utils.health import health_monitor

logger = logging.getLogger(__name__)


async def check_inactivity(bot: Bot, pool: asyncpg.Pool):
    while True:
        health_monitor.update("inactivity_check")
        # Run every 15 minutes instead of every 1 minute for Nano
        await asyncio.sleep(900) 
        try:
            paused, _ = await is_session_paused(pool)
            if paused:
                continue

            config = await get_config(pool)
            inactivity_minutes = int(config.get('inactivity_minutes', '160'))
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=inactivity_minutes)

            kicked = await get_inactive_users(pool, cutoff)

            for user in kicked:
                try:
                    await bot.send_message(
                        user['id'],
                        "💤 <b>You have been marked inactive.</b>\n\n"
                        "You will no longer receive media from others.\n"
                        "Upload media again to reactivate your account.",
                        parse_mode="HTML"
                    )
                except TelegramForbiddenError:
                    await mark_user_blocked(pool, user['id'])
                except Exception:
                    pass

            if kicked:
                logger.info(f"Marked {len(kicked)} users inactive.")

        except Exception as e:
            logger.error(f"Inactivity check error: {safe_error(e)}")
