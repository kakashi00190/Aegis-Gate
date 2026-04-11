import asyncio
import logging
from datetime import datetime, timedelta, timezone
from aiogram import Bot
from aiogram.exceptions import TelegramForbiddenError

import asyncpg
from database import (
    get_config, get_current_session, end_session,
    create_new_session, get_all_notifiable_users, mark_user_blocked
)
from utils.session_announce import broadcast_session_end, broadcast_new_session_started
from utils.helpers import format_timedelta_until
from tasks.cleanup import delete_session_messages

logger = logging.getLogger(__name__)


async def check_session_end(bot: Bot, pool: asyncpg.Pool):
    while True:
        await asyncio.sleep(60)
        try:
            session = await get_current_session(pool)
            if not session:
                continue

            now = datetime.now(timezone.utc)

            if session.get('ended_at'):
                pause_until = session.get('pause_until')
                if not pause_until:
                    new_session = await create_new_session(pool)
                    logger.info(f"Created session #{new_session['session_number']} (no pause_until)")
                    await broadcast_new_session_started(bot, pool, new_session)
                    continue

                if pause_until.tzinfo is None:
                    pause_until = pause_until.replace(tzinfo=timezone.utc)

                if now >= pause_until:
                    new_session = await create_new_session(pool)
                    logger.info(f"Pause ended. Created session #{new_session['session_number']}")
                    await broadcast_new_session_started(bot, pool, new_session)
                continue

            config = await get_config(pool)
            duration_days = int(config.get('session_duration_days', '7'))
            pause_hours = float(config.get('session_pause_hours', '3'))
            top_n = int(config.get('leaderboard_top', '10'))

            started = session['started_at']
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)

            session_end_time = started + timedelta(days=duration_days)
            time_left = session_end_time - now
            
            # Log periodic status if it's been a while
            if not hasattr(check_session_end, 'last_log') or time.monotonic() - check_session_end.last_log > 3600:
                logger.info(f"Session #{session['session_number']} status: Ends in {format_timedelta_until(session_end_time)}")
                check_session_end.last_log = time.monotonic()

            if now >= session_end_time:
                logger.info(f"Auto-ending session #{session['session_number']}")
                result = await end_session(pool, session['id'], pause_hours, top_n)
                if not result:
                    logger.info(f"Session #{session['session_number']} already ended, skipping")
                    continue
                await broadcast_session_end(bot, pool, result, pause_hours)
                logger.info(f"Session #{session['session_number']} ended, pause for {pause_hours}h")
                asyncio.get_running_loop().create_task(
                    delete_session_messages(bot, pool, session['id'])
                )

        except Exception as e:
            logger.error(f"Session check error: {e}")
