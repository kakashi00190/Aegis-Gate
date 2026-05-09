import asyncio
import logging
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters import Command
import asyncpg

from database import get_user, get_user_duplicate_media, delete_user_duplicate_media
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
    
    # Store state for callback
    _cleanup_state[user_id] = {
        'duplicates': duplicates,
        'total_duplicates': total_duplicates,
        'total_media': total_media,
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
        
        # Show deletion animation
        deleting_msg = await callback.message.edit_text(
            "🗑️ <b>Deleting duplicates...</b>\n\n"
            "⚡ Removing redundant files...",
            parse_mode=ParseMode.HTML
        )
        
        # Animate deletion
        frames = ["⚡", "🔥", "💫", "✨"]
        for i, frame in enumerate(frames):
            await asyncio.sleep(0.4)
            try:
                await deleting_msg.edit_text(
                    f"{frame} <b>Deleting duplicates...</b>\n\n"
                    f"{'🗑️' * (i + 1)} Removing redundant files...",
                    parse_mode=ParseMode.HTML
                )
            except:
                pass
        
        # Perform deletion
        file_unique_ids = [d['file_unique_id'] for d in state['duplicates']]
        deleted_count = await delete_user_duplicate_media(pool, user_id, file_unique_ids)
        
        # Show success animation
        success_frames = ["✨", "🎉", "🏆", "💎"]
        for i, frame in enumerate(success_frames):
            await asyncio.sleep(0.3)
            try:
                await deleting_msg.edit_text(
                    f"{frame} <b>Cleanup Complete!</b>\n\n"
                    f"✅ {deleted_count} duplicates removed\n"
                    f"🎯 {len(state['duplicates'])} files cleaned\n"
                    f"💾 Chat optimized",
                    parse_mode=ParseMode.HTML
                )
            except:
                pass
        
        # Final success message
        await deleting_msg.edit_text(
            f"🏆 <b>Cleanup Successful!</b>\n\n"
            f"✅ <b>Results:</b>\n"
            f"• {deleted_count} duplicates deleted\n"
            f"• {len(state['duplicates'])} unique files kept\n"
            f"• {state['total_media'] - deleted_count} total media remaining\n\n"
            f"🎉 Your chat is now clean and optimized!\n"
            f"💎 Original files preserved perfectly",
            parse_mode=ParseMode.HTML
        )
        
        _cleanup_state.pop(user_id, None)
