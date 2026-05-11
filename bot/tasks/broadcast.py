import asyncio
import logging
import time
import random
from typing import List
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter

import asyncpg
# Removed circular import of store_sent_messages_batch
from database import (
    claim_due_broadcasts, get_all_active_users,
    mark_user_blocked, store_sent_message, is_session_paused,
    unclaim_broadcast, get_config, mark_media_sent
)

logger = logging.getLogger(__name__)

from utils.limiter import global_rate_limiter, ban_wave_detector
from utils.helpers import safe_error

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
        logger.error(f"Error in _local_store_sent_messages_batch: {safe_error(e)}")

# --- Anti-violation traffic shaping ---
# Media-type specific concurrency: heavy media gets fewer parallel sends
MEDIA_CONCURRENCY = {
    'photo': 3,
    'video': 2,
    'document': 2,
}
SEND_DELAY_BASE = 0.0        # Rate limiter handles all pacing — no extra delay needed
BATCH_SIZE = 10
MAX_RETRIES = 3
CHUNK_SIZE = 15              # Smaller chunks = fewer concurrent in-flight requests
MAX_RECIPIENTS_PER_ITEM = 60 # Anti-detection: limit recipients per item. 80 was too high.

# Broadcast entropy — varied caption formats to break fingerprint detection
# Different patterns: emoji+name, name only, decorative, subtle, expressive
_ENTROPY_PHRASES = [
    "🔥", "⚡", "📢", "✨", "🎬", "📸", "🎥", "🌟",
    "💫", "🎯", "🪄", "🌙", "🦊", "🐺", "🦅", "🐉",
    "💎", "🎪", "🦋", "🌺", "🍀", "🫧", "🧊", "🪩",
    "🪬", "🝔", "⛧", "⛧", "🜏", "☽", "⚝", "✵",
    "🜿", "◈", "⍟", "⊛", "⎈", "⏣", "⎔", "⍙",
]

# Caption format templates — randomly selected per send
# This prevents all messages having identical structure
_CAPTION_FORMATS = [
    "{emoji} {name}",              # 🔥 UserName
    "{name}",                       # UserName (no emoji)
    "by {name}",                    # by UserName
    "{emoji} {name} {emoji2}",     # 🔥 UserName ⚡
    "— {name}",                     # — UserName
    "{name} ✦",                     # UserName ✦
    "📸 by {name}",                 # 📸 by UserName
    "{emoji2} {name} {emoji}",     # ⚡ UserName 🔥 (reversed)
    "~ {name} ~",                   # ~ UserName ~
    "{name} {emoji2}",              # UserName 🌙
    "via {name}",                   # via UserName
    "✧ {name} ✧",                   # ✧ UserName ✧
    "{emoji} {emoji2} {name}",     # 🔥 ⚡ UserName
    "{name} · {emoji}",            # UserName · 🔥
    "from {name}",                  # from UserName
    "⟡ {name}",                     # ⟡ UserName
    "{emoji} {name}'s",            # 🔥 UserName's
    "「{name}」",                    # 「UserName」
    "{name} ⊹ {emoji2}",           # UserName ⊹ 🌟
    "⌇ {name} {emoji}",            # ⌇ UserName 🔥
]

# Duplicate send protection: tracks (user_id, media_id) recently sent
# Prevents resend loops, accidental rebroadcast, duplicate scheduling bugs
_recent_sends: dict[tuple[int, int], float] = {}  # (user_id, media_id) -> monotonic timestamp
_RECENT_SENDS_TTL = 300  # 5 minutes

def _check_duplicate_send(user_id: int, media_id: int) -> bool:
    """Return True if this (user_id, media_id) was sent recently (duplicate)."""
    now = time.monotonic()
    key = (user_id, media_id)
    last = _recent_sends.get(key)
    if last and now - last < _RECENT_SENDS_TTL:
        return True  # duplicate
    _recent_sends[key] = now
    # Periodic cleanup
    if len(_recent_sends) > 50000:
        expired = [k for k, v in _recent_sends.items() if now - v > _RECENT_SENDS_TTL]
        for k in expired:
            del _recent_sends[k]
    return False

_active_users_cache = {
    'users': [],
    'timestamp': time.monotonic() - 1000
}
CACHE_TTL = 30  # seconds

def invalidate_active_users_cache():
    """Force cache refresh on next read. Call when users are blocked/unblocked."""
    _active_users_cache['timestamp'] = 0

# Module-level media semaphores — populated by process_broadcast_queue on startup
_global_media_semaphores: dict[str, asyncio.Semaphore] = {}

# Track users who blocked bot during current broadcast cycle
# Prevents repeated USER_IS_BLOCKED errors within the same cycle
_blocked_this_cycle: set = set()

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
            logger.error(f"Error in sent_messages_logger_task: {safe_error(e)}")
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
            logger.error(f"Error updating active users cache: {safe_error(e)}")
            # Keep using old cache even if it's expired
            _active_users_cache['timestamp'] = now - (CACHE_TTL / 2) # Retry sooner
            
    return _active_users_cache['users']


from aiogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument, InlineKeyboardMarkup, InlineKeyboardButton

async def send_media_to_user(
    bot: Bot,
    pool: asyncpg.Pool,
    user_id: int,
    media_items: List[dict],
    session_id: int,
    uploader_name: str = '?'
) -> bool:
    """Send one or more media items as individual messages or as a media group."""
    # Duplicate send protection — skip if already sent recently
    for item in media_items:
        if _check_duplicate_send(user_id, item['id']):
            logger.debug(f"Duplicate send skipped: user={user_id} media={item['id']}")
            return True  # treat as success to avoid retry

    # Build uploader credit caption — varied format per send to avoid repetitive patterns
    if uploader_name and uploader_name != '?':
        fmt = random.choice(_CAPTION_FORMATS)
        emoji = random.choice(_ENTROPY_PHRASES)
        emoji2 = random.choice(_ENTROPY_PHRASES)
        credit = fmt.format(emoji=emoji, emoji2=emoji2, name=uploader_name)
    else:
        credit = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Check if this is a media group (album)
            if len(media_items) > 1:
                media_group = []
                for idx, item in enumerate(media_items):
                    m_type = item['media_type']
                    f_id = item['file_id']
                    # Only first item in album gets the caption (Telegram limitation)
                    cap = credit if idx == 0 else None
                    if m_type == 'photo':
                        media_group.append(InputMediaPhoto(media=f_id, caption=cap))
                    elif m_type == 'video':
                        media_group.append(InputMediaVideo(media=f_id, caption=cap))
                    elif m_type == 'document':
                        media_group.append(InputMediaDocument(media=f_id, caption=cap))

                # CRITICAL: send_media_group sends N items as N separate Telegram messages
                # but is only 1 API call. We must consume N tokens to account for the
                # actual message volume — otherwise a 10-item album at 3/sec = 30 msg/sec!
                for _ in range(len(media_items) - 1):  # -1 because _send_with_semaphore already consumed 1
                    await global_rate_limiter.consume()
                
                messages = await bot.send_media_group(user_id, media_group)
                if messages and session_id:
                    for i, msg in enumerate(messages):
                        # Link each message to its specific media_id
                        # Guard against Telegram returning fewer messages than items
                        if i < len(media_items):
                            m_id = media_items[i]['id']
                        else:
                            m_id = None
                        _sent_messages_queue.put_nowait((user_id, msg.message_id, session_id, m_id))
                return True

            # Single media item — use the varied credit format
            item = media_items[0]
            media_type = item['media_type']
            file_id = item['file_id']
            media_id = item['id']
            msg = None

            # Use the already-formatted credit (varied per send)
            caption = credit

            if media_type == 'photo':
                msg = await bot.send_photo(user_id, file_id, caption=caption)
            elif media_type == 'video':
                msg = await bot.send_video(user_id, file_id, caption=caption)
            elif media_type == 'document':
                msg = await bot.send_document(user_id, file_id, caption=caption)
            else:
                return False

            if msg and session_id:
                _sent_messages_queue.put_nowait((user_id, msg.message_id, session_id, media_id))
            return True

        except TelegramRetryAfter as e:
            # Record FloodWait for ban wave detection
            if ban_wave_detector.record_floodwait(e.retry_after):
                # Ban wave just detected - this FloodWait triggered it
                logger.warning(f"FloodWait {e.retry_after}s triggered ban wave detection")
            
            # Apply dynamic slowdown to rate limiter
            global_rate_limiter.apply_flood_pressure()
            # Worker desynchronization: randomized recovery prevents all workers resuming at once
            wait_time = e.retry_after + random.uniform(1.0, 3.0)
            logger.warning(f"Flood control: User {user_id} or Global limit hit. Waiting {wait_time:.1f}s before retry (attempt {attempt}/{MAX_RETRIES}).")
            await asyncio.sleep(wait_time)
            # Re-acquire rate limiter token before retry — without this,
            # all post-FloodWait retries fire at once causing another cascade
            await global_rate_limiter.consume_for_user(user_id)
            continue

        except TelegramForbiddenError as e:
            logger.info(f"User {user_id} blocked the bot (403). Marking as blocked in DB.")
            await mark_user_blocked(pool, user_id)
            invalidate_active_users_cache()  # Remove from next broadcast cycle
            _blocked_this_cycle.add(user_id)  # Skip in current cycle too
            return False

        except TelegramBadRequest as e:
            err_str = str(e)
            # MEDIA_FILE_INVALID = file_id expired/invalid — no point retrying for ANY user
            if 'MEDIA_FILE_INVALID' in err_str:
                raise  # Let broadcast_item handle it (skip entire item)
            # USER_IS_BLOCKED = user blocked the bot — mark and skip, don't retry
            if 'USER_IS_BLOCKED' in err_str:
                logger.info(f"User {user_id} blocked the bot (USER_IS_BLOCKED). Marking as blocked in DB.")
                await mark_user_blocked(pool, user_id)
                invalidate_active_users_cache()  # Remove from next broadcast cycle
                _blocked_this_cycle.add(user_id)  # Skip in current cycle too
                return False
            logger.error(f"Error sending to {user_id}: {safe_error(e)}")
            if attempt < MAX_RETRIES:
                await asyncio.sleep(random.uniform(1.5, 4.0))
            else:
                return False

        except Exception as e:
            err = str(e).lower()
            # Only mark as blocked for very specific Telegram errors that mean the chat is gone
            if any(x in err for x in ['chat not found', 'user_deactivated']):
                logger.info(f"User {user_id} unavailable ({err}). Marking as blocked in DB.")
                await mark_user_blocked(pool, user_id)
                return False
            
            logger.error(f"Error sending to {user_id}: {safe_error(e)}")

            if attempt < MAX_RETRIES:
                # Randomized retry delay instead of fixed 1s
                await asyncio.sleep(random.uniform(1.5, 4.0))
            else:
                return False

    return False


async def _send_with_semaphore(
    semaphore: asyncio.Semaphore,
    bot: Bot,
    pool: asyncpg.Pool,
    user_id: int,
    media_items: List[dict],
    session_id: int,
    uploader_name: str = '?'
) -> bool:
    async with semaphore:
        await global_rate_limiter.consume_for_user(user_id)
        result = await send_media_to_user(bot, pool, user_id, media_items, session_id, uploader_name)
        # Minimal delay only when slowdown is active (FloodWait recovery)
        if global_rate_limiter.slowdown > 1.0:
            jitter = random.uniform(0.8, 1.5)
            delay = 0.05 * jitter * global_rate_limiter.slowdown
            await asyncio.sleep(delay)
        return result


async def broadcast_item(bot: Bot, pool: asyncpg.Pool, media_items: List[dict], recipients: list, semaphore: asyncio.Semaphore):
    # Use info from the first item
    first_item = media_items[0]
    uploader_id = first_item['user_id']
    session_id = first_item['session_id']
    uploader_name = first_item.get('anonymous_name', '?')

    target_users = [r for r in recipients if r['id'] != uploader_id]
    # Filter out users who blocked the bot earlier in this cycle
    if _blocked_this_cycle:
        target_users = [r for r in target_users if r['id'] not in _blocked_this_cycle]
    total_targets = len(target_users)

    if not target_users:
        # If no one to send to, unclaim these items so they stay in queue for later
        m_ids = [item['id'] for item in media_items]
        logger.info(f"No recipients for media from {uploader_name}. Re-queueing {len(m_ids)} items.")
        await unclaim_broadcast(pool, m_ids)
        return

    # Anti-detection: limit recipients per item so not every media goes to ALL users
    # This breaks the pattern of identical recipient lists across all broadcasts
    if total_targets > MAX_RECIPIENTS_PER_ITEM:
        random.shuffle(target_users)
        target_users = target_users[:MAX_RECIPIENTS_PER_ITEM]
        total_targets = len(target_users)

    # Shuffle recipients for organic delivery order — prevents identical send patterns
    random.shuffle(target_users)

    # Select semaphore by media type for pool separation
    media_type = first_item.get('media_type', 'photo')
    type_semaphore = _global_media_semaphores.get(media_type, semaphore)

    start_time = time.monotonic()
    sent_count = 0
    fail_count = 0

    for i in range(0, total_targets, CHUNK_SIZE):
        health_monitor.update("broadcast_queue") # Update health during long broadcasts
        chunk = target_users[i:i + CHUNK_SIZE]
        tasks = [
            _send_with_semaphore(
                type_semaphore, bot, pool,
                recipient['id'], media_items, session_id, uploader_name
            )
            for recipient in chunk
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Check for MEDIA_FILE_INVALID — expired file_id, skip entire item
        if any(isinstance(r, TelegramBadRequest) and 'MEDIA_FILE_INVALID' in str(r) for r in results):
            m_ids = [item['id'] for item in media_items]
            await mark_media_sent(pool, m_ids)
            logger.warning(f"Skipping media from {uploader_name}: MEDIA_FILE_INVALID (expired file_id). Marked {len(m_ids)} item(s) as sent.")
            return

        for r in results:
            if isinstance(r, Exception):
                fail_count += 1
            elif r is True:
                sent_count += 1
            else:
                fail_count += 1

        # Minimal gap between chunks — rate limiter handles pacing
        if i + CHUNK_SIZE < total_targets:
            await asyncio.sleep(random.uniform(0.05, 0.15))

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
    # Per-media-type semaphores for pool separation
    _media_semaphores = {
        'photo': asyncio.Semaphore(MEDIA_CONCURRENCY['photo']),
        'video': asyncio.Semaphore(MEDIA_CONCURRENCY['video']),
        'document': asyncio.Semaphore(MEDIA_CONCURRENCY['document']),
    }
    # Fallback semaphore for unknown types
    default_semaphore = asyncio.Semaphore(MEDIA_CONCURRENCY['photo'])
    # Make semaphores accessible to broadcast_item
    global _global_media_semaphores
    _global_media_semaphores = _media_semaphores

    last_status_log = time.monotonic()
    broadcasts_since_cooldown = 0  # Proactive cooldown counter
    BACKLOG_HIGH_THRESHOLD = 50    # If this many items pending, enter high-throughput mode
    BACKLOG_CRITICAL_THRESHOLD = 200  # If this many items pending, enter critical mode
    BACKLOG_EMERGENCY_THRESHOLD = 2000  # If this many items pending, enter emergency mode

    while True:
        health_monitor.update("broadcast_queue")
        # Clear blocked users set at start of each cycle
        _blocked_this_cycle.clear()
        try:
            paused, _ = await is_session_paused(pool)
            if paused:
                await asyncio.sleep(random.uniform(3, 8))
                continue

            # CRITICAL FIX: Check recipients BEFORE claiming broadcasts
            # This prevents marking media as 'sent' when there's no one to receive it.
            recipients = await get_cached_active_users(pool)
            
            # Diagnostic log
            now = time.monotonic()
            if now - last_status_log > 60:
                logger.info(f"Broadcast loop heart-beat: {len(recipients)} active recipients, slowdown={global_rate_limiter.slowdown:.2f}x")
                last_status_log = now

            if not recipients:
                await asyncio.sleep(random.uniform(5, 15))
                continue

            # Check for ban wave and auto-dodge
            if ban_wave_detector.should_dodge():
                status = ban_wave_detector.get_status()
                dodge_time = status['dodge_remaining']
                logger.warning(f"🛡️ Ban wave dodge active. Pausing broadcasts for {dodge_time:.0f}s")
                await asyncio.sleep(min(60, dodge_time))  # Check every minute
                continue

            # On first iteration after startup, release stale claims from previous instances
            # This must happen BEFORE the first claim so items are available
            if not hasattr(process_broadcast_queue, '_startup_unclaim_done'):
                setattr(process_broadcast_queue, '_startup_unclaim_done', True)
                # Warmup: gradually ramp up rate limiter over 15s to avoid cold-start burst
                # Telegram detects bots that start at full speed immediately after restart
                original_rate = global_rate_limiter.rate
                global_rate_limiter.rate = 1
                global_rate_limiter.tokens = min(global_rate_limiter.tokens, 1)
                logger.info(f"Warmup: starting at 1 msg/sec, ramping to {original_rate} over 30s")
                # Schedule gradual ramp-up in background
                async def _warmup_ramp():
                    steps = [(2, 10), (original_rate, 30)]
                    for target_rate, delay_s in steps:
                        await asyncio.sleep(delay_s)
                        global_rate_limiter.rate = target_rate
                        logger.info(f"Warmup: rate increased to {target_rate} msg/sec")
                asyncio.create_task(_warmup_ramp())

                try:
                    async with pool.acquire() as conn:
                        released = await conn.fetchval(
                            "WITH stale AS ("
                            "  UPDATE media SET claimed_at = NULL "
                            "  WHERE claimed_at IS NOT NULL AND sent_at IS NULL "
                            "  RETURNING id"
                            ") SELECT count(*) FROM stale"
                        )
                        if released:
                            logger.info(f"Startup: released {released} stale-claimed media items back to queue")
                except Exception as e:
                    logger.error(f"Startup unclaim error: {safe_error(e)}")

            # Adaptive throughput: check backlog FIRST to set claim limit and mode
            # Normal (0-49 pending): 3/sec, cooldown every 10, 8-20s gaps
            # High (50-199 pending): 4/sec, cooldown every 15, 5-15s gaps  
            # Critical (200-1999 pending): 5/sec, cooldown every 25, 2-5s gaps
            # Emergency (2000+ pending): 5/sec, cooldown every 30, 1-3s gaps
            try:
                async with pool.acquire() as conn:
                    backlog_count = await conn.fetchval(
                        "SELECT COUNT(*) FROM media WHERE sent_at IS NULL AND scheduled_at <= NOW()"
                    ) or 0
            except:
                backlog_count = 0
            
            if backlog_count >= BACKLOG_EMERGENCY_THRESHOLD:
                effective_rate = 5
                cooldown_n = 30
                cooldown_dur = (20, 40)
                gap_range = (1, 3)
                mode = "EMERGENCY"
            elif backlog_count >= BACKLOG_CRITICAL_THRESHOLD:
                effective_rate = 5
                cooldown_n = 25
                cooldown_dur = (25, 50)
                gap_range = (2, 5)
                mode = "CRITICAL"
            elif backlog_count >= BACKLOG_HIGH_THRESHOLD:
                effective_rate = 4
                cooldown_n = 15
                cooldown_dur = (35, 75)
                gap_range = (5, 15)
                mode = "HIGH"
            else:
                effective_rate = 3
                cooldown_n = 10
                cooldown_dur = (45, 90)
                gap_range = (8, 20)
                mode = "NORMAL"
            
            if global_rate_limiter.rate != effective_rate:
                global_rate_limiter.rate = effective_rate
                global_rate_limiter.capacity = effective_rate
                logger.info(f"📈 Adaptive throughput: {mode} mode (backlog={backlog_count}). Rate={effective_rate}/sec, cooldown every {cooldown_n}, gap={gap_range[0]}-{gap_range[1]}s")

            # Adaptive claim limit: more items when backlog is large
            claim_limit = 25 if backlog_count < BACKLOG_HIGH_THRESHOLD else (40 if backlog_count < BACKLOG_CRITICAL_THRESHOLD else 50)
            raw_items = await claim_due_broadcasts(pool, limit=claim_limit)
            if not raw_items:
                # Periodic status log even if no items
                now = time.monotonic()
                if now - last_status_log > 300:
                    config_data = await get_config(pool)
                    delay = config_data.get('broadcast_delay_seconds', '30')
                    logger.info(f"Broadcast queue: {len(recipients)} recipients, but no items due. (Delay: {delay}s)")
                    last_status_log = now
                await asyncio.sleep(random.uniform(1.5, 4.0))
                continue

            logger.info(f"Claimed {len(raw_items)} media items for broadcast to {len(recipients)} potential recipients. Backlog: {backlog_count} ({mode})")
            last_status_log = time.monotonic()

            # Group items by media_group_id or ID if no media_group_id
            grouped_media = {}
            for item in raw_items:
                group_key = item['media_group_id'] if item['media_group_id'] else f"single_{item['id']}"
                if group_key not in grouped_media:
                    grouped_media[group_key] = []
                grouped_media[group_key].append(dict(item))

            # Batch single items from the same uploader into groups of up to 10
            # This makes single items as fast as albums — 1 broadcast cycle vs N
            singles_by_uploader = {}
            album_groups = {}
            for key, items in grouped_media.items():
                if key.startswith('single_') and len(items) == 1:
                    uid = items[0]['user_id']
                    if uid not in singles_by_uploader:
                        singles_by_uploader[uid] = []
                    singles_by_uploader[uid].append(items[0])
                else:
                    album_groups[key] = items

            # Merge batched singles back into grouped_media
            batched_singles = {}
            for uid, items in singles_by_uploader.items():
                # Telegram allows max 10 items per send_media_group
                # Use 3 max to avoid burst patterns that trigger FloodWait
                for i in range(0, len(items), 3):
                    batch = items[i:i+3]
                    batch_key = f"batch_{uid}_{i}"
                    batched_singles[batch_key] = batch

            grouped_media = {**album_groups, **batched_singles}

            # Process broadcasts sequentially — one at a time.
            # Concurrent broadcasts cause FloodWait avalanche (3 broadcasts
            # fight for same 25 tokens/sec = 75 effective rate = instant flood).
            for items in grouped_media.values():
                # Proactive cooldown: after N broadcasts, take a longer break
                # This prevents the sustained-volume pattern that triggers violations
                broadcasts_since_cooldown += 1
                if broadcasts_since_cooldown >= cooldown_n:
                    cooldown = random.uniform(*cooldown_dur)
                    logger.info(f"🧊 Proactive cooldown: {cooldown:.0f}s after {cooldown_n} broadcasts ({mode} mode)")
                    await asyncio.sleep(cooldown)
                    broadcasts_since_cooldown = 0
                
                await broadcast_item(bot, pool, items, recipients, default_semaphore)
                
                # Anti-detection: random delay between items breaks rapid-fire pattern
                await asyncio.sleep(random.uniform(*gap_range))

        except Exception as e:
            logger.error(f"Broadcast queue error: {safe_error(e)}")
            await asyncio.sleep(random.uniform(3, 10))
