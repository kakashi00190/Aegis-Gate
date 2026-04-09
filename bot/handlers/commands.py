import logging
from datetime import datetime, timedelta, timezone
from aiogram import Router, Bot
from aiogram.filters import Command
from aiogram.types import Message

import asyncpg
from database import (
    get_user, get_user_by_id_or_name, get_user_rank,
    get_leaderboard, get_config, get_current_session, is_session_paused,
    create_report, set_report_admin_message
)
from utils.helpers import (
    get_badge_display, get_all_badges, format_datetime,
    format_timedelta_until, medal_for_rank
)
from utils.levels import format_level_bar
from config import ADMIN_ID

logger = logging.getLogger(__name__)
router = Router()


def build_user_card(user: dict, rank: int, show_session_info: str = "") -> str:
    status_icon = {
        'active': '🟢', 'inactive': '🔴', 'pending': '⏳', 'banned': '⛔'
    }.get(user['status'], '❓')

    badges = get_all_badges(user.get('badge_emoji', ''))
    level = user['level']
    exp = user['exp']
    level_bar = format_level_bar(exp, level)
    blocked_note = " <i>(has blocked bot)</i>" if user.get('bot_blocked') else ""

    card = (
        f"👤 <b>{user['anonymous_name']}</b>{blocked_note}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Status: {status_icon} {user['status'].capitalize()}\n"
        f"🏅 Badges: {badges}\n"
        f"⚡ Level: <b>{level}</b> — EXP: {exp:,}\n"
        f"Progress: {level_bar}\n"
        f"🏆 Session Rank: #{rank}\n"
        f"📊 Session Uploads: {user['session_upload_count']}\n"
        f"📤 Total Lifetime: {user['total_media_lifetime']}\n"
        f"📅 Joined: {format_datetime(user['joined_at'])}\n"
        f"🕐 Last Active: {format_datetime(user['last_activity_at'])}"
    )

    if show_session_info:
        card += f"\n{show_session_info}"

    return card


@router.message(Command("me"))
async def cmd_me(message: Message, pool: asyncpg.Pool):
    user = await get_user(pool, message.from_user.id)
    if not user:
        await message.answer("⚠️ You are not registered. Use /start to begin.")
        return

    rank = await get_user_rank(pool, message.from_user.id)
    config = await get_config(pool)
    session = await get_current_session(pool)
    paused, pause_until = await is_session_paused(pool)

    session_info = ""
    if paused:
        session_info = f"⏸ Session paused — resumes in {format_timedelta_until(pause_until)}"
    elif session:
        duration_days = int(config.get('session_duration_days', '7'))
        started = session['started_at']
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        session_end = started + timedelta(days=duration_days)
        session_info = f"🗓 Session #{session['session_number']} ends: {format_datetime(session_end)}"

    threshold = int(config.get('activation_threshold', '10'))
    user_dict = dict(user)
    if user_dict['status'] == 'pending':
        remaining = max(0, threshold - user_dict['total_media_lifetime'])
        session_info = f"⏳ Upload <b>{remaining}</b> more file(s) to activate."

    card = build_user_card(user_dict, rank, session_info)
    await message.answer(card, parse_mode="HTML")


@router.message(Command("inspect"))
async def cmd_inspect(message: Message, pool: asyncpg.Pool):
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip():
        await message.answer(
            "Usage: /inspect &lt;AnonymousName or ID&gt;\n"
            "Example: /inspect BraveWolf42"
        )
        return

    target_query = args[1].strip()
    target = await get_user_by_id_or_name(pool, target_query)
    if not target:
        await message.answer(f"❌ No user found: <code>{target_query}</code>.")
        return

    rank = await get_user_rank(pool, target['id'])
    card = build_user_card(dict(target), rank)
    await message.answer(card, parse_mode="HTML")


@router.message(Command("leaderboard"))
async def cmd_leaderboard(message: Message, pool: asyncpg.Pool):
    config = await get_config(pool)
    top_n = int(config.get('leaderboard_top', '10'))
    session = await get_current_session(pool)

    leaders = await get_leaderboard(pool, limit=top_n)
    user = await get_user(pool, message.from_user.id)

    session_num = session['session_number'] if session else 1

    lines = [f"🏆 <b>Session #{session_num} Leaderboard</b>\n"]
    for entry in leaders:
        rank = entry['rank']
        medal = medal_for_rank(rank)
        badges = get_badge_display(entry['badge_emoji'], max_shown=3)
        badge_str = f" [{badges}]" if badges != "—" else ""
        lines.append(
            f"{medal} <b>{entry['anonymous_name']}</b>{badge_str} — "
            f"{entry['session_upload_count']} uploads"
        )

    if not leaders:
        lines.append("No uploads this session yet. Be the first!")

    if user and user['status'] != 'banned':
        user_rank = await get_user_rank(pool, message.from_user.id)
        lines.append(
            f"\n📍 Your rank: <b>#{user_rank}</b> "
            f"({user['session_upload_count']} uploads)"
        )

    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("report"))
async def cmd_report(message: Message, pool: asyncpg.Pool, bot: Bot):
    user = await get_user(pool, message.from_user.id)
    if not user:
        await message.answer("⚠️ You are not registered. Use /start to begin.")
        return

    if user['status'] == 'banned':
        return

    if not message.reply_to_message:
        await message.answer(
            "📋 <b>How to report media:</b>\n\n"
            "1. Find the media you want to report\n"
            "2. Tap and hold on it → tap <b>Reply</b>\n"
            "3. Send <b>/report</b> as your reply\n\n"
            "Your report will be reviewed by the admin."
        )
        return

    replied = message.reply_to_message

    file_unique_id = None
    media_type = None
    file_id = None

    if replied.photo:
        file_unique_id = replied.photo[-1].file_unique_id
        file_id = replied.photo[-1].file_id
        media_type = 'photo'
    elif replied.video:
        file_unique_id = replied.video.file_unique_id
        file_id = replied.video.file_id
        media_type = 'video'
    elif replied.document:
        file_unique_id = replied.document.file_unique_id
        file_id = replied.document.file_id
        media_type = 'document'

    if not file_unique_id:
        await message.answer("⚠️ Please reply to a photo, video, or document to report it.")
        return

    async with pool.acquire() as conn:
        media = await conn.fetchrow(
            "SELECT * FROM media WHERE file_unique_id = $1", file_unique_id
        )

        if not media:
            await message.answer("⚠️ This media is not found in the current session or has already been cleared.")
            return

        existing = await conn.fetchval(
            "SELECT 1 FROM reports WHERE reporter_id = $1 AND media_id = $2",
            message.from_user.id, media['id']
        )
        if existing:
            await message.answer("⚠️ You have already reported this media.")
            return

    uploader = await get_user(pool, media['user_id'])
    uploader_name = uploader['anonymous_name'] if uploader else "unknown"

    report = await create_report(
        pool, message.from_user.id, media['id'],
        media['user_id'], uploader_name,
        file_id, media_type
    )

    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Solved", callback_data=f"solve_report_{report['id']}"),
            InlineKeyboardButton(
                text="🚫 Ban Uploader",
                callback_data=f"admin_ban_{media['user_id']}"
            ),
        ]
    ])

    caption = (
        f"🚨 <b>New Report #{report['id']}</b>\n\n"
        f"Uploader: <b>{uploader_name}</b>\n"
        f"Reporter: <b>{user['anonymous_name']}</b>\n"
        f"Media type: {media_type}"
    )

    try:
        if media_type == 'photo':
            msg = await bot.send_photo(ADMIN_ID, file_id, caption=caption,
                                       parse_mode="HTML", reply_markup=kb)
        elif media_type == 'video':
            msg = await bot.send_video(ADMIN_ID, file_id, caption=caption,
                                       parse_mode="HTML", reply_markup=kb)
        else:
            msg = await bot.send_document(ADMIN_ID, file_id, caption=caption,
                                          parse_mode="HTML", reply_markup=kb)
        await set_report_admin_message(pool, report['id'], msg.message_id)
    except Exception as e:
        logger.error(f"Failed to send report to admin: {e}")

    await message.answer("🚨 Report submitted successfully. Thank you.")


@router.message(Command("help"))
async def cmd_help(message: Message, pool: asyncpg.Pool):
    config = await get_config(pool)
    activation_threshold = int(config.get('activation_threshold', '10'))
    reactivation_threshold = int(config.get('reactivation_threshold', '3'))
    inactivity_min = int(config.get('inactivity_minutes', '160'))
    inactivity_h = round(inactivity_min / 60, 1)

    await message.answer(
        "📖 <b>Commands</b>\n\n"
        "/start — Register or check your status\n"
        "/me — Your full profile, EXP, and badges\n"
        "/inspect &lt;name/ID&gt; — View any user's profile\n"
        "/leaderboard — Session top rankings\n"
        "/report — Report inappropriate media\n"
        "/help — This message\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📤 <b>How it works</b>\n"
        f"• Upload <b>{activation_threshold}</b> files to activate your account\n"
        "• Once active, your uploads broadcast to all other active users\n"
        f"• Upload at least once every <b>{inactivity_h}h</b> to stay active\n"
        f"• If you go inactive, upload <b>{reactivation_threshold}</b> file(s) to reactivate\n"
        "• Earn EXP and level up with every upload\n"
        "• Top uploaders each session earn permanent badges\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🚨 <b>How to report media</b>\n"
        "Reply to any media with /report to flag it for admin review.\n\n"
        "🚫 Links and @mentions in captions are not allowed.",
        parse_mode="HTML"
    )
