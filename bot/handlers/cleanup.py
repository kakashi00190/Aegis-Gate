import asyncio
import logging
import random
import time
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters import Command
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
import asyncpg

from database import get_user, get_user_duplicate_media, delete_user_duplicate_messages
from utils.helpers import safe_error

logger = logging.getLogger(__name__)
router = Router()

# Store cleanup state per user
_cleanup_state = {}

@router.message(Command("cleanup"))
async def cleanup_command(message: Message, pool: asyncpg.Pool):
    """Start duplicate media cleanup process"""
    user_id = message.from_user.id
    user = await get_user(pool, user_id)
    
    if not user:
        await message.answer("❌ You need to activate your account first!")
        return
    
    # Show scanning animation
    scanning_msg = await message.answer(
        "🔍 <b>Scanning for duplicates...</b>\n\n"
        "⏳ Analyzing your chat history...",
        parse_mode=ParseMode.HTML
    )
    
    # Animate scanning
    frames = ["⏳", "🔄", "⚡", "✨"]
    for i, frame in enumerate(frames):
        await asyncio.sleep(0.3)
        try:
            await scanning_msg.edit_text(
                f"{frame} <b>Scanning for duplicates...</b>\n\n"
                f"{'🔍' * (i + 1)} Analyzing your chat history...",
                parse_mode=ParseMode.HTML
            )
        except:
            pass
    
    # Find duplicates
    duplicates = await get_user_duplicate_media(pool, user_id)
    
    # Diagnostic: log what we found
    logger.info(f"Cleanup scan for user {user_id}: found {len(duplicates)} duplicate groups")
    
    if not duplicates:
        # Check if user has ANY sent_messages records
        try:
            async with pool.acquire() as conn:
                total_sent = await conn.fetchval(
                    "SELECT COUNT(*) FROM sent_messages WHERE recipient_id = $1", user_id
                )
                total_unique_files = await conn.fetchval(
                    "SELECT COUNT(DISTINCT m.file_unique_id) FROM sent_messages sm JOIN media m ON sm.media_id = m.id WHERE sm.recipient_id = $1 AND m.file_unique_id IS NOT NULL",
                    user_id
                )
                null_file_uid = await conn.fetchval(
                    "SELECT COUNT(*) FROM sent_messages sm JOIN media m ON sm.media_id = m.id WHERE sm.recipient_id = $1 AND m.file_unique_id IS NULL",
                    user_id
                )
                logger.info(f"Cleanup diagnostics for user {user_id}: total_sent={total_sent}, unique_files={total_unique_files}, null_file_unique_id={null_file_uid}")
        except Exception as e:
            logger.error(f"Cleanup diagnostic error: {safe_error(e)}")
        
        await scanning_msg.edit_text(
            "✅ <b>No duplicates found!</b>\n\n"
            "Your chat is clean and organized! 🎉",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Calculate stats
    total_duplicates = sum(d['count'] - 1 for d in duplicates)  # Subtract 1 to keep original
    duplicate_groups = len(duplicates)
    
    # Get total sent messages for context
    try:
        async with pool.acquire() as conn:
            total_sent = await conn.fetchval(
                "SELECT COUNT(*) FROM sent_messages WHERE recipient_id = $1", user_id
            ) or 0
    except:
        total_sent = 0
    
    # Collect message IDs to delete (keep first, delete rest)
    messages_to_delete = []
    for d in duplicates:
        # message_ids[0] is the first (keep), rest are duplicates to delete
        msg_ids = d['message_ids']
        messages_to_delete.extend(msg_ids[1:])  # Skip first, delete rest
    
    # Store state for callback
    _cleanup_state[user_id] = {
        'duplicates': duplicates,
        'total_duplicates': total_duplicates,
        'duplicate_groups': duplicate_groups,
        'total_sent': total_sent,
        'messages_to_delete': messages_to_delete,
        'message_id': scanning_msg.message_id
    }
    
    # Show results with confirmation
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🗑️ Yes, delete duplicates", callback_data="cleanup_confirm"),
        InlineKeyboardButton(text="❌ Keep as is", callback_data="cleanup_cancel")
    )
    
    await scanning_msg.edit_text(
        f"🔍 <b>Duplicate Media Found!</b>\n\n"
        f"📊 <b>Your Chat Stats:</b>\n"
        f"• {total_sent} total media in your chat\n"
        f"• {duplicate_groups} files have duplicate copies\n"
        f"• {total_duplicates} duplicate messages can be removed\n\n"
        f"🎯 <b>What will happen:</b>\n"
        f"• {total_duplicates} duplicate copies deleted from your chat\n"
        f"• {duplicate_groups} original files kept untouched\n"
        f"• {total_sent - total_duplicates} media remaining after cleanup\n\n"
        f"⚠️ <b>This action cannot be undone!</b>\n\n"
        f"Proceed with cleanup?",
        parse_mode=ParseMode.HTML,
        reply_markup=builder.as_markup()
    )

@router.callback_query(F.data.startswith("cleanup_"))
async def cleanup_callback(callback: CallbackQuery, pool: asyncpg.Pool, bot: Bot):
    """Handle cleanup confirmation"""
    user_id = callback.from_user.id
    action = callback.data.split("_")[1]
    
    state = _cleanup_state.get(user_id)
    if not state:
        await callback.answer("❌ Session expired. Start again with /cleanup")
        return
    
    if action == "cancel":
        await callback.message.edit_text(
            "✅ <b>Cleanup cancelled</b>\n\n"
            "Your media remains unchanged. No files were deleted. 🛡️",
            parse_mode=ParseMode.HTML
        )
        _cleanup_state.pop(user_id, None)
        await callback.answer()
        return
    
    if action == "confirm":
        await callback.answer("🗑️ Starting cleanup...")
        
        messages_to_delete = state['messages_to_delete']
        total_to_delete = len(messages_to_delete)
        
        if total_to_delete == 0:
            await callback.message.edit_text(
                "✅ <b>No duplicates to delete!</b>",
                parse_mode=ParseMode.HTML
            )
            _cleanup_state.pop(user_id, None)
            return
        
        # Show deletion progress
        progress_msg = await callback.message.edit_text(
            f"🗑️ <b>Deleting duplicates...</b>\n\n"
            f"⏳ 0/{total_to_delete} removed",
            parse_mode=ParseMode.HTML
        )
        
        # Delete messages from user's Telegram chat one by one
        deleted_count = 0
        failed_count = 0
        batch_size = 5
        logger.info(f"Cleanup: starting deletion of {total_to_delete} duplicate messages for user {user_id}")
        
        for i in range(0, total_to_delete, batch_size):
            batch = messages_to_delete[i:i + batch_size]
            
            for msg_id in batch:
                try:
                    from utils.limiter import global_rate_limiter
                    await global_rate_limiter.consume()
                    await bot.delete_message(user_id, msg_id)
                    deleted_count += 1
                except TelegramBadRequest:
                    # Message already deleted or doesn't exist
                    failed_count += 1
                except TelegramForbiddenError:
                    # Bot can't delete in this chat
                    failed_count += 1
                except Exception as e:
                    logger.debug(f"Failed to delete message {msg_id}: {safe_error(e)}")
                    failed_count += 1
                
                # Small delay to avoid rate limiting
                await asyncio.sleep(0.3)
            
            # Update progress every batch
            processed = min(i + batch_size, total_to_delete)
            logger.info(f"Cleanup progress for user {user_id}: {deleted_count}/{total_to_delete} deleted, {failed_count} failed")
            try:
                await progress_msg.edit_text(
                    f"🗑️ <b>Deleting duplicates...</b>\n\n"
                    f"✅ {deleted_count} removed\n"
                    f"⏳ {processed}/{total_to_delete} processed",
                    parse_mode=ParseMode.HTML
                )
            except:
                pass
        
        # Now clean up the sent_messages records
        db_deleted = 0
        if messages_to_delete:
            db_deleted = await delete_user_duplicate_messages(pool, user_id, messages_to_delete)
            logger.info(f"Cleanup DB: removed {db_deleted} sent_messages records for user {user_id}")
        
        logger.info(f"Cleanup complete for user {user_id}: {deleted_count} Telegram messages deleted, {failed_count} failed, {db_deleted} DB records cleaned")
        
        # Show success animation
        success_frames = ["✨", "🎉", "🏆", "💎"]
        for frame in success_frames:
            await asyncio.sleep(0.3)
            try:
                await progress_msg.edit_text(
                    f"{frame} <b>Cleanup Complete!</b>\n\n"
                    f"✅ {deleted_count} duplicates removed\n"
                    f"🎯 {len(state['duplicates'])} files cleaned\n"
                    f"💾 Chat optimized",
                    parse_mode=ParseMode.HTML
                )
            except:
                pass
        
        # Final success message
        kept_count = state['total_sent'] - deleted_count
        await progress_msg.edit_text(
            f"🏆 <b>Cleanup Successful!</b>\n\n"
            f"✅ <b>Results:</b>\n"
            f"• {deleted_count} duplicate messages deleted from your chat\n"
            f"• {state['duplicate_groups']} original files kept untouched\n"
            f"• {kept_count} total media remaining in your chat\n\n"
            f"🎉 Your chat is now clean and optimized!\n"
            f"💎 No important files were lost",
            parse_mode=ParseMode.HTML
        )
        
        _cleanup_state.pop(user_id, None)


async def auto_cleanup_duplicates_task(bot: Bot, pool: asyncpg.Pool):
    """Background task: continuously cycles through ALL users who have received media,
    silently removing duplicate messages from their Telegram chats (just like /cleanup
    but automatic). After finishing all users, waits a short period then starts over.
    No notifications are sent — duplicates are cleaned silently."""
    
    # Wait 3 minutes after startup before first run
    await asyncio.sleep(180)
    
    # Track per-user last cleanup time to avoid re-scanning too frequently
    _last_cleanup: dict[int, float] = {}
    _MIN_RESCAN_INTERVAL = 3600  # Don't re-scan same user within 1 hour
    
    while True:
        cycle_start = time.time()
        total_cleaned = 0
        total_deleted_msgs = 0
        users_scanned = 0
        
        try:
            # Get ALL users who have ever received media (not just recent 48h)
            async with pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT DISTINCT recipient_id
                    FROM sent_messages
                    ORDER BY recipient_id
                """)
            
            if not rows:
                logger.info("Auto-cleanup: no users with sent_messages found")
                await asyncio.sleep(random.uniform(1800, 3600))
                continue
            
            all_user_ids = [r['recipient_id'] for r in rows]
            logger.info(f"Auto-cleanup: starting cycle over {len(all_user_ids)} total users")
            
            for user_id in all_user_ids:
                # Skip if recently scanned
                last = _last_cleanup.get(user_id, 0)
                if time.time() - last < _MIN_RESCAN_INTERVAL:
                    continue
                
                users_scanned += 1
                
                try:
                    # Find duplicates for this user (same query as /cleanup)
                    duplicates = await get_user_duplicate_media(pool, user_id)
                    
                    if not duplicates:
                        _last_cleanup[user_id] = time.time()
                        continue
                    
                    # Calculate what to delete (keep first, delete rest)
                    messages_to_delete = []
                    for d in duplicates:
                        msg_ids = d['message_ids']
                        messages_to_delete.extend(msg_ids[1:])
                    
                    if not messages_to_delete:
                        _last_cleanup[user_id] = time.time()
                        continue
                    
                    # Delete duplicate messages from user's Telegram chat silently
                    deleted_count = 0
                    blocked = False
                    for msg_id in messages_to_delete:
                        try:
                            from utils.limiter import global_rate_limiter
                            await global_rate_limiter.consume()
                            await bot.delete_message(user_id, msg_id)
                            deleted_count += 1
                        except TelegramBadRequest:
                            pass  # Message already deleted or invalid
                        except TelegramForbiddenError:
                            blocked = True
                            break  # Bot blocked by user, stop trying
                        except Exception:
                            pass
                        await asyncio.sleep(random.uniform(0.25, 0.4))
                    
                    # Clean up sent_messages DB records
                    if messages_to_delete:
                        await delete_user_duplicate_messages(pool, user_id, messages_to_delete)
                    
                    _last_cleanup[user_id] = time.time()
                    
                    if deleted_count > 0:
                        total_cleaned += 1
                        total_deleted_msgs += deleted_count
                        logger.info(f"Auto-cleanup: silently removed {deleted_count} duplicates for user {user_id}")
                    
                    if blocked:
                        continue  # Skip delay, move to next user
                    
                except Exception as e:
                    logger.debug(f"Auto-cleanup error for user {user_id}: {safe_error(e)}")
                    continue
                
                # Small random delay between users to avoid rate limit patterns
                await asyncio.sleep(random.uniform(1.0, 3.0))
            
            cycle_elapsed = time.time() - cycle_start
            logger.info(
                f"Auto-cleanup cycle complete: {users_scanned} users scanned, "
                f"{total_cleaned} users cleaned, {total_deleted_msgs} duplicates removed, "
                f"took {cycle_elapsed:.0f}s"
            )
            
            # If no users needed scanning, wait longer before next cycle
            if users_scanned == 0:
                wait = random.uniform(1800, 3600)  # 30-60 min
            else:
                # Short wait before starting next full cycle
                wait = random.uniform(600, 1800)  # 10-30 min
            
            logger.info(f"Auto-cleanup: next full cycle in {wait/60:.0f} minutes")
            await asyncio.sleep(wait)
        
        except Exception as e:
            logger.error(f"Auto-cleanup task error: {safe_error(e)}")
            await asyncio.sleep(random.uniform(300, 900))  # 5-15 min on error
