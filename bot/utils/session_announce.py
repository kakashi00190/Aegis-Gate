import asyncio
import logging
from datetime import datetime, timezone
from aiogram import Bot
from aiogram.exceptions import TelegramForbiddenError

import asyncpg
from database import get_all_notifiable_users, mark_user_blocked, get_config, get_user
from utils.helpers import format_timedelta_until

logger = logging.getLogger(__name__)


async def _send_all(bot: Bot, pool: asyncpg.Pool, text: str):
    users = await get_all_notifiable_users(pool)
    for user in users:
        try:
            await bot.send_message(user['id'], text, parse_mode="HTML")
        except TelegramForbiddenError:
            await mark_user_blocked(pool, user['id'])
        except Exception:
            pass
        await asyncio.sleep(0.05)


async def broadcast_session_end(bot: Bot, pool: asyncpg.Pool, result: dict, pause_hours: float):
    assignments = result['badge_assignments']
    ended_session = result['ended_session']
    pause_until = result['pause_until']

    lines = [f"🏁 <b>Session #{ended_session['session_number']} Has Ended!</b>\n"]

    if assignments:
        lines.append("🏆 <b>Final Standings — Permanent Badges Awarded:</b>\n")
        for entry in assignments:
            user = entry['user']
            badge = entry['badge']
            rank = entry['rank']
            lines.append(
                f"{badge} #{rank} — <b>{user['anonymous_name']}</b> "
                f"({user['session_upload_count']} uploads)"
            )
    else:
        lines.append("No uploads were made this session.")

    config = await get_config(pool)
    reactivation_threshold = int(config.get('reactivation_threshold', '3'))

    lines.append(
        f"\n⏸ <b>Uploads are paused for {pause_hours}h</b> while all media is being wiped.\n"
        f"💤 Users have been reset to <b>inactive</b>.\n"
        f"🌟 <b>Top 10% active users</b> have remained <b>active</b> as a reward!\n"
        f"📤 Upload <b>{reactivation_threshold}</b> file(s) when uploads resume to reactivate.\n"
        f"🚀 New session starts in {format_timedelta_until(pause_until)}."
    )

    await _send_all(bot, pool, "\n".join(lines))


async def broadcast_new_session_started(bot: Bot, pool: asyncpg.Pool, new_session):
    try:
        session_num = new_session['session_number']
    except (TypeError, KeyError, IndexError):
        session_num = '?'

    config = await get_config(pool)
    activation_threshold = int(config.get('activation_threshold', '10'))
    reactivation_threshold = int(config.get('reactivation_threshold', '3'))

    users = await get_all_notifiable_users(pool)
    for user_row in users:
        user = await get_user(pool, user_row['id'])
        if not user:
            continue

        status = user['status']

        if status == 'active':
            text = (
                f"🚀 <b>Session #{session_num} Has Started!</b>\n\n"
                f"💤 You are currently <b>inactive</b>.\n"
                f"Upload <b>{reactivation_threshold} media file(s)</b> to reactivate and start receiving content again!\n\n"
                f"🏆 Top uploaders earn: 👑 🥈 🥉 🎖️ ⭐"
            )
        elif status == 'inactive':
            text = (
                f"🚀 <b>Session #{session_num} Has Started!</b>\n\n"
                f"💤 You are currently <b>inactive</b>.\n"
                f"Upload <b>{reactivation_threshold} media file(s)</b> to reactivate and start receiving content again!\n\n"
                f"🏆 Top uploaders earn: 👑 🥈 🥉 🎖️ ⭐"
            )
        elif status == 'pending':
            text = (
                f"🚀 <b>Session #{session_num} Has Started!</b>\n\n"
                f"⏳ Your account is still <b>pending</b>.\n"
                f"Upload <b>{activation_threshold} media files</b> to activate your account!"
            )
        else:
            continue

        try:
            await bot.send_message(user['id'], text, parse_mode="HTML")
        except TelegramForbiddenError:
            await mark_user_blocked(pool, user['id'])
        except Exception:
            pass
        await asyncio.sleep(0.05)


async def broadcast_session_results(bot: Bot, pool: asyncpg.Pool, result: dict, pause_hours: float):
    await broadcast_session_end(bot, pool, result, pause_hours)
