import logging
import time
from aiogram import Router, F, Bot
from aiogram.types import Message

import asyncpg
from database import (
    get_user, get_config, get_current_session, is_session_paused,
    add_media, update_user_on_upload, activate_user, reactivate_user,
    increment_inactive_uploads
)
from utils.helpers import contains_link, format_timedelta_until

logger = logging.getLogger(__name__)
router = Router()

_pause_cooldowns: dict[int, float] = {}
_upload_cooldowns: dict[int, list[float]] = {} # user_id -> [timestamps]
COOLDOWN_TTL = 600
MAX_UPLOADS_PER_WINDOW = 15
WINDOW_SECONDS = 10


def _cleanup_cooldowns(cooldowns: dict, ttl: int = COOLDOWN_TTL):
    now = time.time()
    if isinstance(next(iter(cooldowns.values()), None), list):
        # Handle _upload_cooldowns list format
        expired_keys = []
        for k, v in cooldowns.items():
            cooldowns[k] = [ts for v_ts in v if now - v_ts < WINDOW_SECONDS]
            if not cooldowns[k]:
                expired_keys.append(k)
        for k in expired_keys:
            del cooldowns[k]
    else:
        # Handle _pause_cooldowns float format
        expired = [k for k, v in cooldowns.items() if now - v > ttl]
        for k in expired:
            del cooldowns[k]


@router.message(F.content_type.in_({'photo', 'video', 'document'}))
async def handle_media(message: Message, pool: asyncpg.Pool, bot: Bot):
    user_id = message.from_user.id

    user = await get_user(pool, user_id)
    if not user:
        await message.answer("⚠️ You are not registered. Use /start to begin.")
        return

    if user['status'] == 'banned':
        await message.answer("🚫 You are banned from this bot.")
        return

    chat_id = message.chat.id
    caption = message.caption or ""
    if contains_link(caption):
        try:
            await message.delete()
        except Exception:
            pass
        await bot.send_message(
            chat_id,
            "🚫 <b>Caption rejected.</b> Links, usernames, and URLs are not allowed.\n"
            "Remove them and try again.",
            parse_mode="HTML"
        )
        return

    paused, pause_until = await is_session_paused(pool)
    if paused:
        try:
            await message.delete()
        except Exception:
            pass
        now_ts = time.time()
        _cleanup_cooldowns(_pause_cooldowns)
        if now_ts - _pause_cooldowns.get(user_id, 0) > 60:
            _pause_cooldowns[user_id] = now_ts
            time_left = format_timedelta_until(pause_until)
            await bot.send_message(
                chat_id,
                f"⏸ <b>Uploads are paused.</b>\n\n"
                f"Session is transitioning. Media is being wiped.\n"
                f"Uploads resume in <b>{time_left}</b>.",
                parse_mode="HTML"
            )
        return

    if message.content_type == 'photo':
        file_id = message.photo[-1].file_id
        file_unique_id = message.photo[-1].file_unique_id
        media_type = 'photo'
    elif message.content_type == 'video':
        file_id = message.video.file_id
        file_unique_id = message.video.file_unique_id
        media_type = 'video'
    else:
        file_id = message.document.file_id
        file_unique_id = message.document.file_unique_id
        media_type = 'document'

    config = await get_config(pool)
    activation_threshold = int(config.get('activation_threshold', '10'))
    reactivation_threshold = int(config.get('reactivation_threshold', '3'))
    delay = int(config.get('broadcast_delay_seconds', '30'))

    session = await get_current_session(pool)
    if not session:
        await message.answer("⚠️ No active session. Please try again later.")
        return
    session_id = session['id']
    media_group_id = message.media_group_id

    # Silent EXP update
    stats = await update_user_on_upload(pool, user_id)
    if not stats:
        return

    updated_user = stats['user']
    level_up = stats['level_up']
    new_level = stats['new_level']

    if user['status'] == 'pending':
        new_total = updated_user['total_media_lifetime']
        if new_total >= activation_threshold:
            activated = await activate_user(pool, user_id)
            if activated:
                await add_media(
                    pool, user_id, session_id, 
                    file_id, file_unique_id, media_type, 
                    delay, media_group_id
                )
                await message.answer(
                    "✅ <b>You are now active!</b>\n\n"
                    "You will start receiving media from other users.\n"
                    "Inactivity timer has started — keep uploading to stay active.",
                    parse_mode="HTML"
                )
        else:
            remaining = activation_threshold - new_total
            # Only notify every 5 uploads to avoid flooding user in high-volume bursts
            if new_total % 5 == 0 or remaining < 3:
                await message.answer(
                    f"📤 <b>Upload received!</b> {new_total}/{activation_threshold}\n"
                    f"Upload <b>{remaining}</b> more file(s) to activate your account.",
                    parse_mode="HTML"
                )
        if level_up:
            await message.answer(
                f"🎉 <b>Level Up!</b> You are now <b>Level {new_level}</b>.",
                parse_mode="HTML"
            )
        return

    if user['status'] == 'inactive':
        count = await increment_inactive_uploads(pool, user_id)
        if count >= reactivation_threshold:
            reactivated = await reactivate_user(pool, user_id)
            if reactivated:
                await add_media(
                    pool, user_id, session_id, 
                    file_id, file_unique_id, media_type, 
                    delay, media_group_id
                )
                await message.answer(
                    "✅ <b>You have been reactivated!</b>\n\n"
                    "You will receive media from other users again.\n"
                    "Inactivity timer has restarted.",
                    parse_mode="HTML"
                )
        else:
            remaining = reactivation_threshold - count
            # Only notify once during reactivation burst
            if count == 1:
                await message.answer(
                    f"📤 <b>Upload received!</b> {count}/{reactivation_threshold}\n"
                    f"Upload <b>{remaining}</b> more file(s) to reactivate your account.",
                    parse_mode="HTML"
                )
        if level_up:
            await message.answer(
                f"🎉 <b>Level Up!</b> You are now <b>Level {new_level}</b>.",
                parse_mode="HTML"
            )
        return

    if user['status'] == 'active':
        await add_media(
            pool, user_id, session_id,
            file_id, file_unique_id, media_type, 
            delay, media_group_id
        )
        if level_up:
            await message.answer(
                f"🎉 <b>Level Up!</b> You are now <b>Level {new_level}</b>.",
                parse_mode="HTML"
            )
