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
    
    if not duplicates:
        await scanning_msg.edit_text(
            "✅ <b>No duplicates found!</b>\n\n"
            "Your chat is clean and organized! 🎉",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Calculate stats
    total_duplicates = sum(d['count'] - 1 for d in duplicates)  # Subtract 1 to keep original
    total_media = sum(d['count'] for d in duplicates)
    
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
        'total_media': total_media,
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
        f"📊 <b>Statistics:</b>\n"
        f"• {len(duplicates)} files have duplicates\n"
        f"• {total_media} total media files\n"
        f"• {total_duplicates} can be safely removed\n\n"
        f"🎯 <b>What will be deleted:</b>\n"
        f"• Only duplicate copies\n"
        f"• Original files kept\n"
        f"• Nothing important lost\n\n"
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
            await delete_user_duplicate_messages(pool, user_id, messages_to_delete)
        
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
        kept_count = state['total_media'] - deleted_count
        await progress_msg.edit_text(
            f"🏆 <b>Cleanup Successful!</b>\n\n"
            f"✅ <b>Results:</b>\n"
            f"• {deleted_count} duplicates deleted from your chat\n"
            f"• {len(state['duplicates'])} unique files kept\n"
            f"• {kept_count} total media remaining\n\n"
            f"🎉 Your chat is now clean and optimized!\n"
            f"💎 Original files preserved perfectly",
            parse_mode=ParseMode.HTML
        )
        
        _cleanup_state.pop(user_id, None)
