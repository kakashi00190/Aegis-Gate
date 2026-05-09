import asyncio
import logging
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
