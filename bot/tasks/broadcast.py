import asyncio
import logging
import time
import random
from typing import List
from aiogram import Bot
from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter

import asyncpg
# Removed circular import of store_sent_messages_batch
from database import (
    claim_due_broadcasts, get_all_active_users,
    mark_user_blocked, store_sent_message, is_session_paused,
    unclaim_broadcast, get_config, mark_media_sent
)

logger = logging.getLogger(__name__)

from utils.limiter import global_rate_limiter

# Define local batch storer to break circular dependency
async def _local_store_sent_messages_batch(pool: asyncpg.Pool, batch: List[tuple]):
    if not batch:
        return
    try:
        async with asyncio.timeout(20):
            async with pool.acquire() as conn:
                await conn.executemany(
                    "INSERT INTO sent_messages (recipient_id, message_id, session_id, media_id) "
                    "VALUES ($1, $2, $3, $4)",
                    batch
                )
    except Exception as e:
        logger.error(f"Error in _local_store_sent_messages_batch: {repr(e)}")

SEND_CONCURRENCY = 15
SEND_DELAY_BASE = 0.05
BATCH_SIZE = 10
MAX_RETRIES = 3
CHUNK_SIZE = 20

_active_users_cache = {
    'users': [],
    'timestamp': time.monotonic() - 1000
}
CACHE_TTL = 30  # seconds

from utils.health import health_monitor

# Global queue for logging sent messages to database
_sent_messages_queue = asyncio.Queue()

async def sent_messages_logger_task(pool: asyncpg.Pool):
    """Background task to batch insert sent messages into database."""
    batch = []
    last_flush = time.monotonic()
    
    while True:
        health_monitor.update("sent_messages_logger")
        try:
            # Wait for an item or timeout
            try:
                item = await asyncio.wait_for(_sent_messages_queue.get(), timeout=1.0)
                batch.append(item)
            except asyncio.TimeoutError:
                pass

            # Flush batch if it's large enough or enough time has passed
            now = time.monotonic()
            if batch and (len(batch) >= 100 or now - last_flush >= 2.0):
                await _local_store_sent_messages_batch(pool, batch)
                batch = []
                last_flush = now
                
        except Exception as e:
            logger.error(f"Error in sent_messages_logger_task: {repr(e)}")
            await asyncio.sleep(1)

async def get_cached_active_users(pool: asyncpg.Pool):
    now = time.monotonic()
    if now - _active_users_cache['timestamp'] > CACHE_TTL:
        try:
            users = await get_all_active_users(pool)
            # Update cache always if we got a successful query
            _active_users_cache['users'] = users
            _active_users_cache['timestamp'] = now
        except Exception as e:
            logger.error(f"Error updating active users cache: {repr(e)}")
            # Keep using old cache even if it's expired
            _active_users_cache['timestamp'] = now - (CACHE_TTL / 2) # Retry sooner
            
    return _active_users_cache['users']


from aiogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument, InlineKeyboardMarkup, InlineKeyboardButton

async def send_media_to_user(
    bot: Bot,
    pool: asyncpg.Pool,
    user_id: int,
    media_items: List[dict],
    session_id: int
) -> bool:
    """Send one or more media items as individual messages or as a media group."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Check if this is a media group (album)
            if len(media_items) > 1:
                media_group = []
                for item in media_items:
                    m_type = item['media_type']
                    f_id = item['file_id']
                    if m_type == 'photo':
                        media_group.append(InputMediaPhoto(media=f_id))
                    elif m_type == 'video':
                        media_group.append(InputMediaVideo(media=f_id))
                    elif m_type == 'document':
                        media_group.append(InputMediaDocument(media=f_id))

                messages = await bot.send_media_group(user_id, media_group)
                if messages and session_id:
                    for i, msg in enumerate(messages):
                        # Link each message to its specific media_id
                        m_id = media_items[i]['id']
                        _sent_messages_queue.put_nowait((user_id, msg.message_id, session_id, m_id))
                return True

            # Single media item
            item = media_items[0]
            media_type = item['media_type']
            file_id = item['file_id']
            media_id = item['id']
            msg = None

            if media_type == 'photo':
                msg = await bot.send_photo(user_id, file_id)
            elif media_type == 'video':
                msg = await bot.send_video(user_id, file_id)
            elif media_type == 'document':
                msg = await bot.send_document(user_id, file_id)
            else:
                return False

            if msg and session_id:
                _sent_messages_queue.put_nowait((user_id, msg.message_id, session_id, media_id))
            return True

        except TelegramRetryAfter as e:
            wait_time = min(e.retry_after + 1, 10)
            logger.warning(f"Rate limited (user {user_id}), waiting {wait_time}s")
            await asyncio.sleep(wait_time)
            continue

        except TelegramForbiddenError as e:
            logger.info(f"User {user_id} blocked the bot (403). Marking as blocked in DB.")
            await mark_user_blocked(pool, user_id)
            return False

        except Exception as e:
            err = str(e).lower()
            # Only mark as blocked for very specific Telegram errors that mean the chat is gone
            if any(x in err for x in ['chat not found', 'user_deactivated']):
                logger.info(f"User {user_id} unavailable ({err}). Marking as blocked in DB.")
                await mark_user_blocked(pool, user_id)
                return False
            
            logger.error(f"Error sending to {user_id}: {repr(e)}")

            if attempt < MAX_RETRIES:
                await asyncio.sleep(1)
            else:
                return False

    return False


async def _send_with_semaphore(
    semaphore: asyncio.Semaphore,
    bot: Bot,
    pool: asyncpg.Pool,
    user_id: int,
    media_items: List[dict],
    session_id: int
) -> bool:
    async with semaphore:
        await global_rate_limiter.consume()
        result = await send_media_to_user(bot, pool, user_id, media_items, session_id)
        jitter = random.uniform(0.8, 1.2)
        await asyncio.sleep(SEND_DELAY_BASE * jitter)
        return result


async def broadcast_item(bot: Bot, pool: asyncpg.Pool, media_items: List[dict], recipients: list, semaphore: asyncio.Semaphore):
    # Use info from the first item
    first_item = media_items[0]
    uploader_id = first_item['user_id']
    session_id = first_item['session_id']
    uploader_name = first_item.get('anonymous_name', '?')

    target_users = [r for r in recipients if r['id'] != uploader_id]
    total_targets = len(target_users)

    if not target_users:
        # If no one to send to, unclaim these items so they stay in queue for later
        m_ids = [item['id'] for item in media_items]
        logger.info(f"No recipients for media from {uploader_name}. Re-queueing {len(m_ids)} items.")
        await unclaim_broadcast(pool, m_ids)
        return

    start_time = time.monotonic()
    sent_count = 0
    fail_count = 0

    for i in range(0, total_targets, CHUNK_SIZE):
        health_monitor.update("broadcast_queue") # Update health during long broadcasts
        chunk = target_users[i:i + CHUNK_SIZE]
        tasks = [
            _send_with_semaphore(
                semaphore, bot, pool,
                recipient['id'], media_items, session_id
            )
            for recipient in chunk
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if r is True:
                sent_count += 1
            else:
                fail_count += 1

    elapsed = round(time.monotonic() - start_time, 1)
    type_label = "album" if len(media_items) > 1 else "media"
    
    # Mark as sent in DB now that we've actually delivered (or tried to)
    m_ids = [item['id'] for item in media_items]
    await mark_media_sent(pool, m_ids)
    
    logger.info(
        f"Broadcast complete: {type_label} from {uploader_name} -> "
        f"{sent_count}/{total_targets} delivered, "
        f"{fail_count} failed, {elapsed}s elapsed"
    )


async def process_broadcast_queue(bot: Bot, pool: asyncpg.Pool):
    semaphore = asyncio.Semaphore(SEND_CONCURRENCY)
    last_status_log = time.monotonic()

    while True:
        health_monitor.update("broadcast_queue")
        try:
            paused, _ = await is_session_paused(pool)
            if paused:
                await asyncio.sleep(5)
                continue

            # CRITICAL FIX: Check recipients BEFORE claiming broadcasts
            # This prevents marking media as 'sent' when there's no one to receive it.
            recipients = await get_cached_active_users(pool)
            
            # Diagnostic log
            now = time.monotonic()
            if now - last_status_log > 60: # More frequent logs for debugging
                logger.info(f"Broadcast loop heart-beat: {len(recipients)} active recipients.")
                last_status_log = now

            if not recipients:
                await asyncio.sleep(10)
                continue

            # Limit how many items we process at once to avoid DB/Network overload
            raw_items = await claim_due_broadcasts(pool, limit=10) # Reduced from BATCH_SIZE (20)
            if not raw_items:
                # Periodic status log even if no items
                now = time.monotonic()
                if now - last_status_log > 300: # Log every 5 mins
                    config_data = await get_config(pool)
                    delay = config_data.get('broadcast_delay_seconds', '30')
                    logger.info(f"Broadcast queue: {len(recipients)} recipients, but no items due. (Delay: {delay}s)")
                    last_status_log = now
                await asyncio.sleep(2) # Wait slightly longer
                continue

            logger.info(f"Claimed {len(raw_items)} media items for broadcast to {len(recipients)} potential recipients.")
            last_status_log = time.monotonic()

            # Group items by media_group_id or ID if no media_group_id
            grouped_media = {}
            for item in raw_items:
                group_key = item['media_group_id'] if item['media_group_id'] else f"single_{item['id']}"
                if group_key not in grouped_media:
                    grouped_media[group_key] = []
                grouped_media[group_key].append(dict(item))

            tasks = [
                broadcast_item(bot, pool, items, recipients, semaphore)
                for items in grouped_media.values()
            ]
            await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            logger.error(f"Broadcast queue error: {repr(e)}")
            await asyncio.sleep(5)
