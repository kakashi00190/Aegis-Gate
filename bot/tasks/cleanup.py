import asyncio
import logging
import time
from aiogram import Bot
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError, TelegramBadRequest
import asyncpg

from database import (
    get_session_sent_messages_batch, delete_sent_messages_batch,
    get_all_notifiable_users, mark_user_blocked, get_config,
    get_all_sent_messages_batch
)

from utils.health import health_monitor

logger = logging.getLogger(__name__)

CLEANUP_CONCURRENCY = 8
CLEANUP_BATCH_SIZE = 200
DELETE_DELAY = 0.035


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
            # Handle rate limit from Telegram specifically
            await asyncio.sleep(e.retry_after)
            try:
                await bot.delete_message(chat_id, message_id)
                return True
            except Exception:
                return False
        except (TelegramBadRequest, TelegramForbiddenError):
            return False
        except Exception as e:
            logger.error(f"Cleanup error for user {chat_id}: {e}")
            return False


async def _count_total_messages(pool: asyncpg.Pool, session_id: int) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT COUNT(*) FROM sent_messages WHERE session_id = $1",
            session_id
        ) or 0


def generate_progress_bar(pct: int, length: int = 15) -> str:
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
            # ONLY send real-time progress to the Admin to avoid rate limits
            from config import ADMIN_ID
            if user['id'] == ADMIN_ID:
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
        await asyncio.sleep(0.02)

    semaphore = asyncio.Semaphore(CLEANUP_CONCURRENCY)
    last_pct = -1

    while True:
        batch = await get_session_sent_messages_batch(pool, session_id, limit=CLEANUP_BATCH_SIZE)
        if not batch:
            break

        batch_ids = [row['id'] for row in batch]
        tasks = [
            _delete_one_message(semaphore, bot, row['recipient_id'], row['message_id'])
            for row in batch
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        deleted_in_batch = sum(1 for r in results if r is True)
        skipped_in_batch = len(results) - deleted_in_batch

        total_deleted += deleted_in_batch
        total_skipped += skipped_in_batch
        total_processed += len(batch)

        await delete_sent_messages_batch(pool, batch_ids)

        pct = min(100, int(total_processed / total_messages * 100)) if total_messages > 0 else 100
        if pct > last_pct or total_processed >= total_messages:
            last_pct = pct
            bar = generate_progress_bar(pct)
            
            update_text = (
                f"🧹 <b>Wiping session media...</b>\n\n"
                f"⏳ Progress: {total_processed}/{total_messages} ({pct}%)\n"
                f"<code>{bar}</code>\n\n"
                f"🗑 Deleted: {total_deleted} | ⏭ Skipped: {total_skipped}"
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
            f"({total_deleted} deleted, {total_skipped} skipped) — {elapsed}s elapsed"
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
            await bot.send_message(user['id'], done_text, parse_mode="HTML")
        except TelegramForbiddenError:
            await mark_user_blocked(pool, user['id'])
        except Exception:
            pass
        await asyncio.sleep(0.03)


async def emergency_wipe_all(bot: Bot, pool: asyncpg.Pool, admin_msg=None):
    start_time = time.monotonic()
    total_deleted = 0
    total_skipped = 0
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
            # ONLY send real-time progress to the Admin to avoid rate limits
            from config import ADMIN_ID
            if user['id'] == ADMIN_ID:
                msg = await bot.send_message(
                    user['id'],
                    f"🚨 <b>EMERGENCY MEDIA WIPE</b>\n\n"
                    f"⏳ Progress: 0/{total_messages} (0%)\n"
                    f"[░░░░░░░░░░]\n\n"
                    f"⚠️ All media is being deleted by admin.",
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
        await asyncio.sleep(0.03)

    semaphore = asyncio.Semaphore(CLEANUP_CONCURRENCY)
    last_pct = 0

    while True:
        batch = await get_all_sent_messages_batch(pool, limit=CLEANUP_BATCH_SIZE)
        if not batch:
            break

        batch_ids = [row['id'] for row in batch]
        tasks = [
            _delete_one_message(semaphore, bot, row['recipient_id'], row['message_id'])
            for row in batch
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        deleted_in_batch = sum(1 for r in results if r is True)
        skipped_in_batch = len(results) - deleted_in_batch

        total_deleted += deleted_in_batch
        total_skipped += skipped_in_batch
        total_processed += len(batch)

        await delete_sent_messages_batch(pool, batch_ids)

        pct = min(100, int(total_processed / total_messages * 100)) if total_messages > 0 else 100
        if pct > last_pct or total_processed >= total_messages:
            last_pct = pct
            bar = generate_progress_bar(pct)
            elapsed = round(time.monotonic() - start_time, 1)

            update_text = (
                f"🚨 <b>EMERGENCY MEDIA WIPE</b>\n\n"
                f"⏳ Progress: {total_processed}/{total_messages} ({pct}%)\n"
                f"<code>{bar}</code>\n\n"
                f"🗑 Deleted: {total_deleted} | ⏭ Skipped: {total_skipped}\n"
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
                    f"⏳ {total_processed}/{total_messages} ({pct}%)\n"
                    f"<code>{bar}</code>\n\n"
                    f"🗑 Deleted: {total_deleted} | ⏭ Skipped: {total_skipped}\n"
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
            logger.error(f"Error in cleanup_stale_verifications_task: {e}")


async def _count_all_messages(pool: asyncpg.Pool) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM sent_messages") or 0
