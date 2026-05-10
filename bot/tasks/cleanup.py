import asyncio
import logging
import time
import random
from datetime import datetime, timedelta, timezone
from aiogram import Bot
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError, TelegramBadRequest
import asyncpg

from database import (
    get_session_sent_messages_batch, delete_sent_messages_batch,
    get_all_notifiable_users, mark_user_blocked, get_config,
    get_all_sent_messages_batch, get_media_delete_stats,
    delete_single_media_db, get_user_sent_messages_for_purge,
    delete_user_sent_messages_batch, delete_user_sent_media,
    purge_user_queued_media
)

from utils.health import health_monitor
from utils.helpers import format_timedelta_until, safe_error

logger = logging.getLogger(__name__)

CLEANUP_CONCURRENCY = 4
CLEANUP_BATCH_SIZE = 100
DELETE_DELAY = 0.05


from utils.limiter import global_rate_limiter

async def _delete_one_message(
    semaphore: asyncio.Semaphore,
    bot: Bot,
    chat_id: int,
    message_id: int
) -> bool:
    async with semaphore:
        # Rate limiting
        await global_rate_limiter.consume()
        
        try:
            await bot.delete_message(chat_id, message_id)
            return True
        except TelegramRetryAfter as e:
            # Apply dynamic slowdown + randomized recovery (desync workers)
            global_rate_limiter.apply_flood_pressure()
            wait = e.retry_after + random.uniform(0.5, 2.0)
            logger.warning(f"Cleanup flood control: waiting {wait:.1f}s")
            await asyncio.sleep(wait)
            try:
                await bot.delete_message(chat_id, message_id)
                return True
            except Exception:
                return False
        except (TelegramBadRequest, TelegramForbiddenError):
            return False
        except Exception as e:
            logger.error(f"Cleanup error for user {chat_id}: {safe_error(e)}")
            return False


async def _count_total_messages(pool: asyncpg.Pool, session_id: int) -> int:
    try:
        async with asyncio.timeout(15):
            async with pool.acquire() as conn:
                return await conn.fetchval(
                    "SELECT COUNT(*) FROM sent_messages WHERE session_id = $1",
                    session_id
                ) or 0
    except Exception as e:
        logger.error(f"Error counting messages for cleanup: {safe_error(e)}")
        return 0


def generate_progress_bar(pct: int, length: int = 15) -> str:
    if pct <= 0:
        return "░" * length
    filled = int((pct / 100) * length)
    chars = ["░", "▏", "▎", "▍", "▌", "▋", "▊", "▉", "█"]
    
    full_blocks = filled
    if full_blocks >= length:
        return "█" * length
        
    remainder = (pct * length / 100) - full_blocks
    char_idx = int(remainder * 8)
    
    bar = "█" * full_blocks + chars[char_idx] + "░" * (length - full_blocks - 1)
    return bar


async def delete_session_messages(bot: Bot, pool: asyncpg.Pool, session_id: int):
    start_time = time.monotonic()
    total_deleted = 0
    total_skipped = 0
    total_db_cleaned = 0
    total_processed = 0

    total_messages = await _count_total_messages(pool, session_id)

    if total_messages == 0:
        await _broadcast_cleanup_done(bot, pool)
        return

    users = await get_all_notifiable_users(pool)
    progress_msgs = {}

    initial_bar = generate_progress_bar(0)
    for user in users:
        try:
            from utils.limiter import global_rate_limiter
            await global_rate_limiter.consume_for_user(user['id'])
            # ONLY send real-time progress to the Admin to avoid rate limits
            from config import is_admin
            if is_admin(user['id']):
                msg = await bot.send_message(
                    user['id'],
                    f"🧹 <b>Wiping session media...</b>\n\n"
                    f"⏳ Progress: 0/{total_messages} (0%)\n"
                    f"<code>{initial_bar}</code>",
                    parse_mode="HTML"
                )
                progress_msgs[user['id']] = msg
            else:
                # Other users just get a one-time message
                await bot.send_message(
                    user['id'],
                    f"🧹 <b>Wiping session media...</b>\n\n"
                    "Please wait while all media from the previous session is being cleared.\n"
                    "Uploads will resume shortly.",
                    parse_mode="HTML"
                )
        except TelegramForbiddenError:
            await mark_user_blocked(pool, user['id'])
        except Exception:
            pass

    semaphore = asyncio.Semaphore(CLEANUP_CONCURRENCY)
    last_update_processed = 0
    last_update_time = time.monotonic()

    while True:
        batch = await get_session_sent_messages_batch(pool, session_id, limit=CLEANUP_BATCH_SIZE)
        if not batch:
            break

        batch_ids = [row['id'] for row in batch]

        # Try to delete ALL messages from chats — recent ones succeed,
        # old ones (>48h) fail with TelegramBadRequest and get counted as skipped.
        tasks = [
            _delete_one_message(semaphore, bot, row['recipient_id'], row['message_id'])
            for row in batch
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        deleted_in_batch = sum(1 for r in results if r is True)
        skipped_in_batch = len(batch) - deleted_in_batch

        total_deleted += deleted_in_batch
        total_skipped += skipped_in_batch
        total_db_cleaned += len(batch)
        total_processed += len(batch)

        # Always clean DB records (frees storage regardless of chat deletion)
        await delete_sent_messages_batch(pool, batch_ids)

        pct_float = (total_processed / total_messages * 100.0) if total_messages > 0 else 100.0
        pct = min(100, max(0, int(pct_float)))
        now = time.monotonic()
        should_update = (
            (total_processed - last_update_processed) >= 1000
            or (now - last_update_time) >= 5.0
            or total_processed >= total_messages
        )
        if should_update:
            last_update_processed = total_processed
            last_update_time = now
            bar = generate_progress_bar(pct)
            elapsed = round(time.monotonic() - start_time, 1)

            update_text = (
                f"🧹 <b>Wiping session media...</b>\n\n"
                f"⏳ Progress: {total_processed}/{total_messages} ({pct_float:.2f}%)\n"
                f"<code>{bar}</code>\n\n"
                f"🗑 Chat deleted: {total_deleted} | ⏭ Skipped: {total_skipped}\n"
                f"💾 DB cleaned: {total_db_cleaned}"
            )

            for uid, msg in list(progress_msgs.items()):
                try:
                    await msg.edit_text(update_text, parse_mode="HTML")
                except (TelegramBadRequest, TelegramForbiddenError):
                    pass
                except Exception:
                    pass

        elapsed = round(time.monotonic() - start_time, 1)
        logger.info(
            f"Cleanup session {session_id}: {total_processed} processed "
            f"({total_deleted} chat-deleted, {total_skipped} skipped, {total_db_cleaned} db-cleaned) — {elapsed}s elapsed"
        )

        await asyncio.sleep(0.2)

    elapsed = round(time.monotonic() - start_time, 1)
    logger.info(
        f"Session {session_id} cleanup complete: "
        f"{total_deleted} deleted, {total_skipped} skipped "
        f"({total_processed} total) in {elapsed}s"
    )

    config = await get_config(pool)
    reactivation_threshold = int(config.get('reactivation_threshold', '3'))

    done_text = (
        f"✅ <b>Session media wiped!</b>\n\n"
        f"🗑 {total_deleted} messages deleted in {elapsed}s\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💤 Users have been reset to <b>inactive</b>.\n"
        f"🌟 <b>Top 10% active users</b> have remained <b>active</b> as a reward!\n"
        f"📤 Upload <b>{reactivation_threshold}</b> media file(s) to reactivate and start receiving content again!"
    )

    for uid, msg in progress_msgs.items():
        try:
            await msg.edit_text(done_text, parse_mode="HTML")
        except (TelegramBadRequest, TelegramForbiddenError):
            pass
        except Exception:
            pass

    for user in users:
        if user['id'] not in progress_msgs:
            try:
                from utils.limiter import global_rate_limiter
                await global_rate_limiter.consume_for_user(user['id'])
                await bot.send_message(user['id'], done_text, parse_mode="HTML")
            except Exception:
                pass


async def _broadcast_cleanup_done(bot: Bot, pool: asyncpg.Pool):
    config = await get_config(pool)
    reactivation_threshold = int(config.get('reactivation_threshold', '3'))

    done_text = (
        f"✅ <b>Session complete!</b>\n\n"
        f"💤 Users have been reset to <b>inactive</b>.\n"
        f"🌟 <b>Top 10% active users</b> have remained <b>active</b> as a reward!\n"
        f"📤 Upload <b>{reactivation_threshold}</b> media file(s) to reactivate and start receiving content again!"
    )

    users = await get_all_notifiable_users(pool)
    for user in users:
        try:
            from utils.limiter import global_rate_limiter
            await global_rate_limiter.consume_for_user(user['id'])
            await bot.send_message(user['id'], done_text, parse_mode="HTML")
        except TelegramForbiddenError:
            await mark_user_blocked(pool, user['id'])
        except Exception:
            pass


async def emergency_wipe_all(bot: Bot, pool: asyncpg.Pool, admin_msg=None):
    start_time = time.monotonic()
    total_deleted = 0
    total_skipped = 0
    total_db_cleaned = 0
    total_processed = 0

    total_messages = await _count_all_messages(pool)

    if total_messages == 0:
        if admin_msg:
            try:
                await admin_msg.edit_text(
                    "⚠️ <b>Emergency Wipe</b>\n\nNo tracked messages to delete.",
                    parse_mode="HTML"
                )
            except Exception:
                pass
        return {'deleted': 0, 'skipped': 0, 'total': 0, 'elapsed': 0}

    users = await get_all_notifiable_users(pool)
    progress_msgs = {}

    for user in users:
        try:
            from utils.limiter import global_rate_limiter
            await global_rate_limiter.consume_for_user(user['id'])
            # ONLY send real-time progress to the Admin to avoid rate limits
            from config import is_admin
            if is_admin(user['id']):
                msg = await bot.send_message(
                    user['id'],
                    f"🚨 <b>EMERGENCY MEDIA WIPE</b>\n\n"
                    f"⏳ Progress: 0/{total_messages} (0%)\n"
                    f"<code>{generate_progress_bar(0)}</code>",
                    parse_mode="HTML"
                )
                progress_msgs[user['id']] = msg
            else:
                # Other users just get a one-time message
                await bot.send_message(
                    user['id'],
                    "🚨 <b>EMERGENCY MEDIA WIPE</b>\n\n"
                    "Admin has initiated a full media wipe. Please wait.",
                    parse_mode="HTML"
                )
        except TelegramForbiddenError:
            await mark_user_blocked(pool, user['id'])
        except Exception:
            pass

    semaphore = asyncio.Semaphore(CLEANUP_CONCURRENCY)
    last_update_processed = 0
    last_update_time = time.monotonic()

    while True:
        batch = await get_all_sent_messages_batch(pool, limit=CLEANUP_BATCH_SIZE)
        if not batch:
            break

        batch_ids = [row['id'] for row in batch]

        # Try to delete ALL messages from chats — recent ones will succeed,
        # old ones (>48h) will fail with TelegramBadRequest and be counted as skipped.
        # We don't preemptively skip because some messages near the 48h boundary
        # might still be deletable, and it's better to try than to assume.
        tasks = [
            _delete_one_message(semaphore, bot, row['recipient_id'], row['message_id'])
            for row in batch
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        deleted_in_batch = sum(1 for r in results if r is True)
        skipped_in_batch = len(batch) - deleted_in_batch

        total_deleted += deleted_in_batch
        total_skipped += skipped_in_batch
        total_db_cleaned += len(batch)
        total_processed += len(batch)

        # Always clean DB records (frees storage regardless of chat deletion)
        await delete_sent_messages_batch(pool, batch_ids)

        pct_float = (total_processed / total_messages * 100.0) if total_messages > 0 else 100.0
        pct = min(100, max(0, int(pct_float)))
        now = time.monotonic()
        should_update = (
            (total_processed - last_update_processed) >= 1000
            or (now - last_update_time) >= 5.0
            or total_processed >= total_messages
        )
        if should_update:
            last_update_processed = total_processed
            last_update_time = now
            bar = generate_progress_bar(pct)
            elapsed = round(time.monotonic() - start_time, 1)

            update_text = (
                f"🚨 <b>EMERGENCY MEDIA WIPE</b>\n\n"
                f"⏳ Progress: {total_processed}/{total_messages} ({pct_float:.2f}%)\n"
                f"<code>{bar}</code>\n\n"
                f"🗑 Chat deleted: {total_deleted} | ⏭ Skipped: {total_skipped}\n"
                f"💾 DB cleaned: {total_db_cleaned}\n"
                f"⏱ Elapsed: {elapsed}s"
            )

            for uid, msg in list(progress_msgs.items()):
                try:
                    await msg.edit_text(update_text, parse_mode="HTML")
                except (TelegramBadRequest, TelegramForbiddenError):
                    pass
                except Exception:
                    pass

            if admin_msg:
                admin_update = (
                    f"🚨 <b>Emergency Wipe In Progress</b>\n\n"
                    f"⏳ {total_processed}/{total_messages} ({pct_float:.2f}%)\n"
                    f"<code>{bar}</code>\n\n"
                    f"🗑 Chat: {total_deleted} | ⏭ Skip: {total_skipped} | 💾 DB: {total_db_cleaned}\n"
                    f"⏱ Elapsed: {elapsed}s"
                )
                try:
                    await admin_msg.edit_text(admin_update, parse_mode="HTML")
                except Exception:
                    pass

        await asyncio.sleep(0.2)

    elapsed = round(time.monotonic() - start_time, 1)
    logger.info(
        f"Emergency wipe complete: "
        f"{total_deleted} deleted, {total_skipped} skipped "
        f"({total_processed} total) in {elapsed}s"
    )

    config = await get_config(pool)
    reactivation_threshold = int(config.get('reactivation_threshold', '3'))

    done_text = (
        f"🚨 <b>EMERGENCY WIPE COMPLETE</b>\n\n"
        f"🗑 {total_deleted} messages deleted in {elapsed}s\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"All media has been cleared from your chat.\n"
        f"📤 Upload <b>{reactivation_threshold}</b> media file(s) to reactivate and start receiving content!"
    )

    for uid, msg in progress_msgs.items():
        try:
            await msg.edit_text(done_text, parse_mode="HTML")
        except (TelegramBadRequest, TelegramForbiddenError):
            pass
        except Exception:
            pass

    for user in users:
        if user['id'] not in progress_msgs:
            try:
                from utils.limiter import global_rate_limiter
                await global_rate_limiter.consume_for_user(user['id'])
                await bot.send_message(user['id'], done_text, parse_mode="HTML")
            except Exception:
                pass

    return {
        'deleted': total_deleted,
        'skipped': total_skipped,
        'total': total_processed,
        'elapsed': elapsed
    }


async def cleanup_stale_verifications_task(pool: asyncpg.Pool):
    """Background task to remove pending verifications older than 15 minutes."""
    while True:
        health_monitor.update("stale_verifications_cleanup")
        await asyncio.sleep(300) # Run every 5 minutes
        try:
            async with pool.acquire() as conn:
                deleted = await conn.execute(
                    "DELETE FROM pending_verifications WHERE created_at < NOW() - INTERVAL '15 minutes'"
                )
                if deleted != "DELETE 0":
                    logger.info(f"Cleaned up stale verifications: {deleted}")
        except Exception as e:
            logger.error(f"Error in cleanup_stale_verifications_task: {safe_error(e)}")


async def cleanup_48hr_media_task(pool: asyncpg.Pool):
    """Background task to purge media and sent_messages older than 48 hours.
    Telegram bots cannot delete messages older than 48h, so keeping these
    records serves no purpose and wastes database storage."""
    while True:
        await asyncio.sleep(3600)  # Run every 1 hour
        try:
            async with pool.acquire() as conn:
                # Delete sent_messages older than 48h
                sm_deleted = await conn.execute(
                    "DELETE FROM sent_messages WHERE sent_at < NOW() - INTERVAL '48 hours'"
                )
                # Delete media records older than 48h that have been sent
                media_deleted = await conn.execute(
                    "DELETE FROM media WHERE sent_at IS NOT NULL AND sent_at < NOW() - INTERVAL '48 hours'"
                )
                if sm_deleted != "DELETE 0" or media_deleted != "DELETE 0":
                    logger.info(f"48hr cleanup: sent_messages {sm_deleted}, media {media_deleted}")
        except Exception as e:
            logger.error(f"Error in cleanup_48hr_media_task: {safe_error(e)}")


async def _count_all_messages(pool: asyncpg.Pool) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM sent_messages") or 0


async def delete_media_sent_messages(bot: Bot, pool: asyncpg.Pool, media_id: int) -> dict:
    """Delete a single media's sent messages from recipient chats, then remove from DB.
    Also deletes the original uploader's message if tracked.
    Returns dict with deleted/skipped counts."""
    stats = await get_media_delete_stats(pool, media_id)
    if not stats:
        logger.warning(f"delete_media_sent_messages: media {media_id} not found in DB")
        return {'deleted': 0, 'skipped': 0, 'total': 0, 'error': 'Media not found'}

    media = stats['media']
    chat_deleted = 0
    chat_skipped = 0
    original_deleted = False

    # 1. Delete the original uploader's message from their chat
    if media.get('original_chat_id') and media.get('original_message_id'):
        try:
            from utils.limiter import global_rate_limiter
            await global_rate_limiter.consume()
            await bot.delete_message(media['original_chat_id'], media['original_message_id'])
            original_deleted = True
            logger.info(f"Deleted original upload message for media {media_id} from chat {media['original_chat_id']}")
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
            try:
                await global_rate_limiter.consume()
                await bot.delete_message(media['original_chat_id'], media['original_message_id'])
                original_deleted = True
            except Exception:
                logger.warning(f"Failed to delete original message for media {media_id} after retry")
        except Exception as e:
            logger.warning(f"Could not delete original upload message for media {media_id}: {safe_error(e)}")

    # 2. If queued (never sent), just delete from DB
    if stats['is_queued']:
        ok = await delete_single_media_db(pool, media_id)
        logger.info(f"Deleted queued media {media_id}: success={ok}")
        return {'deleted': 1 if original_deleted else 0, 'skipped': 0, 'total': 1 if ok else 0,
                'queued_removed': ok, 'original_deleted': original_deleted}

    # 3. If sent, delete broadcast messages from recipient chats
    semaphore = asyncio.Semaphore(CLEANUP_CONCURRENCY)
    offset = 0

    while True:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, recipient_id, message_id FROM sent_messages WHERE media_id = $1 "
                "ORDER BY id ASC LIMIT 200 OFFSET $2",
                media_id, offset
            )
        if not rows:
            break

        tasks = [
            _delete_one_message(semaphore, bot, row['recipient_id'], row['message_id'])
            for row in rows
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        chat_deleted += sum(1 for r in results if r is True)
        chat_skipped += sum(1 for r in results if r is not True)
        offset += 200

    logger.info(f"Deleted media {media_id} from chats: {chat_deleted} deleted, {chat_skipped} skipped, original_deleted={original_deleted}")

    # 4. Now delete from DB (sent_messages + media row + solve reports)
    await delete_single_media_db(pool, media_id)

    return {
        'deleted': chat_deleted,
        'skipped': chat_skipped,
        'total': chat_deleted + chat_skipped,
        'original_deleted': original_deleted,
    }


async def purge_user_sent_messages(bot: Bot, pool: asyncpg.Pool, user_id: int) -> dict:
    """Delete all of a user's sent media messages from recipient chats, then remove from DB.
    Also deletes original uploader messages if tracked.
    Returns dict with deleted/skipped/queued counts."""
    semaphore = asyncio.Semaphore(CLEANUP_CONCURRENCY)
    chat_deleted = 0
    chat_skipped = 0
    total_processed = 0
    original_deleted = 0

    # 1. Delete original uploader messages from their chat
    try:
        async with pool.acquire() as conn:
            originals = await conn.fetch(
                "SELECT id, original_chat_id, original_message_id FROM media "
                "WHERE user_id = $1 AND original_chat_id IS NOT NULL AND original_message_id IS NOT NULL",
                user_id
            )
        for row in originals:
            try:
                from utils.limiter import global_rate_limiter
                await global_rate_limiter.consume()
                await bot.delete_message(row['original_chat_id'], row['original_message_id'])
                original_deleted += 1
            except Exception:
                pass
        if originals:
            logger.info(f"Purge user {user_id}: deleted {original_deleted}/{len(originals)} original upload messages")
    except Exception as e:
        logger.warning(f"Purge user {user_id}: error fetching original messages: {safe_error(e)}")

    # 2. Delete broadcast messages from recipient chats in batches
    offset = 0
    while True:
        rows = await get_user_sent_messages_for_purge(pool, user_id, limit=CLEANUP_BATCH_SIZE, offset=offset)
        if not rows:
            break

        tasks = [
            _delete_one_message(semaphore, bot, row['recipient_id'], row['message_id'])
            for row in rows
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        chat_deleted += sum(1 for r in results if r is True)
        chat_skipped += sum(1 for r in results if r is not True)
        total_processed += len(rows)

        offset += CLEANUP_BATCH_SIZE
        await asyncio.sleep(0.2)

    # 3. Delete sent_messages from DB
    db_sent_deleted = await delete_user_sent_messages_batch(pool, user_id)

    # 4. Delete sent media rows from DB
    db_media_deleted = await delete_user_sent_media(pool, user_id)

    # 5. Delete queued (unsent) media from DB
    queued_deleted = await purge_user_queued_media(pool, user_id)

    logger.info(f"Purge user {user_id} complete: chat_deleted={chat_deleted}, chat_skipped={chat_skipped}, "
                f"original_deleted={original_deleted}, queued={queued_deleted}, db_media={db_media_deleted}")

    return {
        'chat_deleted': chat_deleted,
        'chat_skipped': chat_skipped,
        'db_sent_deleted': db_sent_deleted,
        'db_media_deleted': db_media_deleted,
        'queued_deleted': queued_deleted,
        'original_deleted': original_deleted,
        'total': total_processed + queued_deleted,
    }
