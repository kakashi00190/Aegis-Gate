import logging
import time
import asyncio
from aiogram import Router, F, Bot
from aiogram.types import Message
from aiogram.exceptions import TelegramRetryAfter

import asyncpg
from database import (
    get_user, get_config, get_current_session, is_session_paused,
    add_media, update_user_on_upload, activate_user, reactivate_user,
    increment_inactive_uploads, get_upload_context,
    get_referral_count, get_missed_media_for_user
)
from utils.helpers import contains_link, format_timedelta_until, safe_error
from utils.levels import referral_badge_for_count, referral_exp_bonus, REFERRAL_BADGES
from config import is_admin

logger = logging.getLogger(__name__)
router = Router()


async def _award_referral_bonus(bot: Bot, pool: asyncpg.Pool, newly_active_user_id: int):
    """Award EXP and badge to referrer when their referral activates."""
    try:
        user = await get_user(pool, newly_active_user_id)
        if not user or not user.get('referred_by'):
            return
        # Already awarded? Skip
        if user.get('referral_awarded'):
            return
        referrer_id = user['referred_by']

        referrer = await get_user(pool, referrer_id)
        if not referrer:
            return

        bonus = referral_exp_bonus(referrer['level'])
        async with pool.acquire() as conn:
            # Mark as awarded FIRST to prevent double-award race
            await conn.execute(
                "UPDATE users SET referral_awarded = TRUE WHERE id = $1",
                newly_active_user_id
            )
            await conn.execute(
                "UPDATE users SET exp = exp + $2 WHERE id = $1",
                referrer_id, bonus
            )
            # Check for referral badge upgrade
            ref_count = await get_referral_count(pool, referrer_id)
            badge_info = referral_badge_for_count(ref_count)
            if badge_info:
                emoji, title = badge_info
                existing_badges = referrer.get('badge_emoji') or ''
                if emoji not in existing_badges:
                    new_badges = f"{existing_badges},{emoji}" if existing_badges else emoji
                    await conn.execute(
                        "UPDATE users SET badge_emoji = $1 WHERE id = $2",
                        new_badges, referrer_id
                    )
                    try:
                        await bot.send_message(
                            referrer_id,
                            f"🏅 <b>New Referral Badge Unlocked!</b>\n\n"
                            f"{emoji} <b>{title}</b>\n"
                            f"You've referred <b>{ref_count}</b> active users!\n\n"
                            f"+{bonus} EXP awarded for this referral.",
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass
                else:
                    try:
                        await bot.send_message(
                            referrer_id,
                            f"🎁 <b>Referral Activated!</b>\n\n"
                            f"Your referral just activated their account!\n"
                            f"+{bonus} EXP",
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass
            else:
                try:
                    await bot.send_message(
                        referrer_id,
                        f"🎁 <b>Referral Activated!</b>\n\n"
                        f"Your referral just activated their account!\n"
                        f"+{bonus} EXP\n\n"
                        f"Refer {REFERRAL_BADGES[0][0]} people to unlock your first badge!",
                        parse_mode="HTML"
                    )
                except Exception:
                    pass
    except Exception as e:
        logger.error(f"Error awarding referral bonus for {newly_active_user_id}: {safe_error(e)}")


async def _deliver_missed_media(bot: Bot, pool: asyncpg.Pool, user_id: int):
    """Deliver media items the user missed while inactive. Runs in background."""
    try:
        missed = await get_missed_media_for_user(pool, user_id, limit=20)
        if not missed:
            return
        logger.info(f"Delivering {len(missed)} missed media items to reactivated user {user_id}")
        from utils.limiter import global_rate_limiter
        for item in missed:
            try:
                await global_rate_limiter.consume()
                media_type = item.get('media_type', 'photo')
                file_id = item['file_id']
                uploader_name = item.get('anonymous_name', '?')
                import random as _r
                from tasks.broadcast import _ENTROPY_PHRASES, _CAPTION_FORMATS
                fmt = _r.choice(_CAPTION_FORMATS)
                emoji = _r.choice(_ENTROPY_PHRASES)
                emoji2 = _r.choice(_ENTROPY_PHRASES)
                credit = fmt.format(emoji=emoji, emoji2=emoji2, name=uploader_name) if uploader_name and uploader_name != '?' else None
                if media_type == 'photo':
                    await bot.send_photo(user_id, file_id, caption=credit)
                elif media_type == 'video':
                    await bot.send_video(user_id, file_id, caption=credit)
                elif media_type == 'document':
                    await bot.send_document(user_id, file_id, caption=credit)
                # Small delay between missed media sends to avoid burst
                await asyncio.sleep(_r.uniform(0.3, 0.8))
            except Exception as e:
                err = str(e).lower()
                if 'blocked' in err or 'deactivated' in err or 'not found' in err:
                    logger.info(f"User {user_id} unavailable during missed media delivery. Stopping.")
                    return
                # Skip this item, continue with next
                logger.debug(f"Skipping missed media {item['id']} for user {user_id}: {safe_error(e)}")
        logger.info(f"Finished delivering missed media to user {user_id}")
    except Exception as e:
        logger.error(f"Error delivering missed media to {user_id}: {safe_error(e)}")


async def _safe_answer(message: Message, text: str, **kwargs):
    """Send a message.answer() with TelegramRetryAfter handling."""
    try:
        await message.answer(text, **kwargs)
    except TelegramRetryAfter as e:
        logger.warning(f"Flood control on handler reply, waiting {e.retry_after}s")
        await asyncio.sleep(e.retry_after)
        try:
            await message.answer(text, **kwargs)
        except Exception:
            pass
    except Exception:
        pass


async def _safe_send(bot: Bot, chat_id: int, text: str, **kwargs):
    """Send a bot.send_message() with TelegramRetryAfter handling."""
    try:
        await bot.send_message(chat_id, text, **kwargs)
    except TelegramRetryAfter as e:
        logger.warning(f"Flood control on bot send, waiting {e.retry_after}s")
        await asyncio.sleep(e.retry_after)
        try:
            await bot.send_message(chat_id, text, **kwargs)
        except Exception:
            pass
    except Exception:
        pass

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
        await _safe_answer(message, "⚠️ Database is busy. Please try again in a moment.")
        return

    user = context['user']
    session = context['session']
    config = context['config']

    if not user:
        await _safe_answer(message, "⚠️ You are not registered. Use /start to begin.")
        return

    if user['status'] == 'banned':
        await _safe_answer(message, "🚫 You are banned from this bot.")
        return

    chat_id = message.chat.id
    caption = message.caption or ""
    if contains_link(caption):
        try:
            await message.delete()
        except Exception:
            pass
        await _safe_send(bot, chat_id,
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
            await _safe_send(bot, chat_id,
                f"⏸ <b>Uploads are paused.</b>\n\n"
                f"Session is transitioning. Media is being wiped.\n"
                f"Uploads resume in <b>{time_left}</b>.",
                parse_mode="HTML"
            )
        return

    if message.content_type not in {'photo', 'video', 'document'}:
        # Notify about unsupported media types (but don't delete unless paused)
        await _safe_answer(message,
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
        await _safe_answer(message, "⚠️ No active session. Please try again later.")
        return

    activation_threshold = int(config.get('activation_threshold', '10'))
    reactivation_threshold = int(config.get('reactivation_threshold', '3'))
    delay = int(config.get('broadcast_delay_seconds', '30'))
    logger.debug(f"Broadcast delay from config: {delay}s (raw: '{config.get('broadcast_delay_seconds', '30')}')")

    session_id = session['id']
    media_group_id = message.media_group_id

    # 5. Atomic Update and Save
    stats = await update_user_on_upload(pool, user_id)
    if not stats:
        return

    updated_user = stats['user']
    level_up = stats['level_up']
    new_level = stats['new_level']

    # 6. Add to media queue (ALL uploads go to queue regardless of status)
    await add_media(
        pool, user_id, session_id,
        file_id, file_unique_id, media_type,
        delay, media_group_id,
        original_chat_id=chat_id,
        original_message_id=message.message_id
    )

    if user['status'] == 'pending':
        new_total = updated_user['total_media_lifetime']
        if new_total >= activation_threshold:
            activated = await activate_user(pool, user_id)
            if activated:
                # Award referral bonus to referrer (if any) now that user is active
                await _award_referral_bonus(bot, pool, user_id)
                await _safe_answer(message,
                    "✅ <b>You are now active!</b>\n\n"
                    "You will start receiving media from other users.\n"
                    "Inactivity timer has started — keep uploading to stay active.",
                    parse_mode="HTML"
                )
        else:
            remaining = activation_threshold - new_total
            # Only notify every 5 uploads to avoid flooding user in high-volume bursts
            if new_total % 5 == 0 or remaining < 3:
                await _safe_answer(message,
                    f"📤 <b>Upload received!</b> {new_total}/{activation_threshold}\n"
                    f"Upload <b>{remaining}</b> more file(s) to activate your account.",
                    parse_mode="HTML"
                )
        if level_up:
            await _safe_answer(message,
                f"🎉 <b>Level Up!</b> You are now <b>Level {new_level}</b>.",
                parse_mode="HTML"
            )
        return

    if user['status'] == 'inactive':
        count = await increment_inactive_uploads(pool, user_id)
        if count >= reactivation_threshold:
            reactivated = await reactivate_user(pool, user_id)
            if reactivated:
                # Deliver missed media in background — user gets what they missed while inactive
                asyncio.create_task(_deliver_missed_media(bot, pool, user_id))
                await _safe_answer(message,
                    "✅ <b>You have been reactivated!</b>\n\n"
                    "You will receive media from other users again.\n"
                    "Inactivity timer has restarted.",
                    parse_mode="HTML"
                )
        else:
            remaining = reactivation_threshold - count
            # Only notify once during reactivation burst
            if count == 1:
                await _safe_answer(message,
                    f"📤 <b>Upload received!</b> {count}/{reactivation_threshold}\n"
                    f"Upload <b>{remaining}</b> more file(s) to reactivate your account.",
                    parse_mode="HTML"
                )
        if level_up:
            await _safe_answer(message,
                f"🎉 <b>Level Up!</b> You are now <b>Level {new_level}</b>.",
                parse_mode="HTML"
            )
        return

    if user['status'] == 'active':
        if level_up:
            await _safe_answer(message,
                f"🎉 <b>Level Up!</b> You are now <b>Level {new_level}</b>.",
                parse_mode="HTML"
            )
