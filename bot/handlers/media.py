import logging
import time
from aiogram import Router, F, Bot
from aiogram.types import Message

import asyncpg
from database import (
    get_user, get_config, get_current_session, is_session_paused,
    add_media, update_user_on_upload, activate_user, reactivate_user,
    increment_inactive_uploads, get_upload_context
)
from utils.helpers import contains_link, format_timedelta_until
from config import ADMIN_ID

logger = logging.getLogger(__name__)
router = Router()

def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID

_pause_cooldowns: dict[int, float] = {}
_upload_cooldowns: dict[int, list[float]] = {} # user_id -> [timestamps]
COOLDOWN_TTL = 600
# Effectively unlimited for premium experience: 100,000 uploads per 60 seconds
MAX_UPLOADS_PER_WINDOW = 100000 
WINDOW_SECONDS = 60


def _cleanup_cooldowns(cooldowns: dict, ttl: int = COOLDOWN_TTL):
    now = time.time()
    if isinstance(next(iter(cooldowns.values()), None), list):
        # Handle _upload_cooldowns list format
        expired_keys = []
        for k, v in cooldowns.items():
            cooldowns[k] = [v_ts for v_ts in v if now - v_ts < WINDOW_SECONDS]
            if not cooldowns[k]:
                expired_keys.append(k)
        for k in expired_keys:
            del cooldowns[k]
    else:
        # Handle _pause_cooldowns float format
        expired = [k for k, v in cooldowns.items() if now - v > ttl]
        for k in expired:
            del cooldowns[k]


@router.message(F.content_type.in_({
    'photo', 'video', 'document', 'audio', 'voice', 
    'animation', 'sticker', 'video_note'
}))
async def handle_media(message: Message, pool: asyncpg.Pool, bot: Bot):
    # 1. Rate Limiting Check
    user_id = message.from_user.id
    now_ts = time.time()
    user_uploads = _upload_cooldowns.get(user_id, [])
    # Keep only uploads within the last WINDOW_SECONDS
    user_uploads = [ts for ts in user_uploads if now_ts - ts < WINDOW_SECONDS]
    
    if len(user_uploads) >= MAX_UPLOADS_PER_WINDOW:
        # Silently ignore extreme floods to protect database
        return
    
    user_uploads.append(now_ts)
    _upload_cooldowns[user_id] = user_uploads

    # 2. Get User, Session, and Config in ONE optimized call
    context = await get_upload_context(pool, user_id)
    if not context['success']:
        await message.answer("⚠️ Database is busy. Please try again in a moment.")
        return

    user = context['user']
    session = context['session']
    config = context['config']

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

    # 3. Check Session Status
    # is_session_paused still uses the DB, but we already have the session object!
    # Let's check pause status locally if possible.
    paused = False
    pause_until = None
    if session and session['pause_until']:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        pause_until = session['pause_until']
        if pause_until.tzinfo is None:
            pause_until = pause_until.replace(tzinfo=timezone.utc)
        if now < pause_until:
            paused = True

    if paused and not is_admin(user_id):
        try:
            await message.delete()
        except Exception:
            pass
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

    if message.content_type not in {'photo', 'video', 'document'}:
        # Notify about unsupported media types (but don't delete unless paused)
        await message.answer(
            "⚠️ <b>Unsupported media type.</b>\n\n"
            "Only photos, videos, and documents are shared with other users.",
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

    if not session:
        await message.answer("⚠️ No active session. Please try again later.")
        return

    activation_threshold = int(config.get('activation_threshold', '10'))
    reactivation_threshold = int(config.get('reactivation_threshold', '3'))
    delay = int(config.get('broadcast_delay_seconds', '30'))

    session_id = session['id']
    media_group_id = message.media_group_id

    # 5. Atomic Update and Save
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
