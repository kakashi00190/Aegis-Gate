import asyncio
import logging
import random
from datetime import datetime, timedelta, timezone

from aiogram import Router, F, Bot
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

import asyncpg
from database import (
    get_config, set_config, get_advanced_stats, get_unsolved_reports, count_unsolved_reports,
    solve_report, get_user, get_user_by_id_or_name, ban_user, unban_user,
    get_current_session, end_session, get_session_stats, is_session_paused,
    create_report, set_report_admin_message, mark_user_blocked,
    get_wipe_stats, reset_all_blocked_status
)
from utils.helpers import format_datetime, format_timedelta_until, get_all_badges
from utils.session_announce import broadcast_session_results
from tasks.cleanup import delete_session_messages, emergency_wipe_all
from config import ADMIN_ID

logger = logging.getLogger(__name__)
router = Router()


class AdminSettingState(StatesGroup):
    entering_value = State()


class AdminUserState(StatesGroup):
    searching = State()


class AdminAnnounceState(StatesGroup):
    entering_message = State()
    confirming = State()


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def admin_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Stats", callback_data="admin_stats"),
            InlineKeyboardButton(text="⚙️ Settings", callback_data="admin_settings"),
        ],
        [
            InlineKeyboardButton(text="🚨 Reports", callback_data="admin_reports_0"),
            InlineKeyboardButton(text="👥 Users", callback_data="admin_users"),
        ],
        [
            InlineKeyboardButton(text="📢 Announce", callback_data="admin_announce"),
            InlineKeyboardButton(text="🏆 Session", callback_data="admin_session"),
        ],
        [
            InlineKeyboardButton(text="🚨 Emergency Wipe", callback_data="admin_emergency_wipe"),
        ],
    ])


def back_keyboard(cb: str = "admin_main") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Back", callback_data=cb)]
    ])


async def _send_fresh(source, text: str, reply_markup=None, parse_mode="HTML", loading_msg=None):
    if loading_msg:
        try:
            await loading_msg.delete()
        except Exception:
            pass
    if isinstance(source, CallbackQuery):
        try:
            await source.message.delete()
        except Exception:
            pass
        return await source.message.answer(text, parse_mode=parse_mode, reply_markup=reply_markup)
    else:
        return await source.answer(text, parse_mode=parse_mode, reply_markup=reply_markup)


async def _show_loading(callback: CallbackQuery, label: str = "Loading") -> Message:
    try:
        await callback.message.delete()
    except Exception:
        pass
    return await callback.message.answer(
        f"⏳ <b>{label}...</b>",
        parse_mode="HTML"
    )


@router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer("<b>🛠 Admin Panel</b>", parse_mode="HTML",
                         reply_markup=admin_main_keyboard())


@router.callback_query(F.data == "admin_main")
async def admin_main(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.clear()
    await _send_fresh(callback, "<b>🛠 Admin Panel</b>", reply_markup=admin_main_keyboard())
    await callback.answer()


@router.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery, pool: asyncpg.Pool):
    if not is_admin(callback.from_user.id):
        return

    loading = await _show_loading(callback, "Loading stats")
    await callback.answer()

    try:
        async with asyncio.timeout(12): # slightly longer than db timeout
            s = await get_advanced_stats(pool)
    except asyncio.TimeoutError:
        await loading.edit_text(
            "⚠️ <b>Stats Loading Timeout</b>\n\nThe database took too long to respond. Please try again in a few moments.",
            reply_markup=admin_main_keyboard()
        )
        return

    if s.get('status') != 'ok':
        error_msg = s.get('error', 'Unknown error')
        await loading.edit_text(
            f"❌ <b>Error Loading Stats</b>\n\n<code>{error_msg}</code>\n\nPlease contact the developer if this persists.",
            reply_markup=admin_main_keyboard()
        )
        return

    session = s['session']
    config = await get_config(pool)

    total = s['total']
    # Active percentage based on total users (not including unverified)
    active_pct = round(s['active'] / s['total_users'] * 100) if s['total_users'] > 0 else 0

    paused, pause_until = await is_session_paused(pool)

    session_line = ""
    if session:
        started = session.get('started_at')
        if started:
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)
            duration_days = int(config.get('session_duration_days', '7'))
            session_end = started + timedelta(days=duration_days)
            now = datetime.now(timezone.utc)
            running_for = now - started
            days_running = running_for.days
            ends_in = format_timedelta_until(session_end)
            session_line = (
                f"\n🏆 <b>Session #{session.get('session_number', '?')}</b>\n"
                f"  Running: {days_running}d | Ends in: {ends_in}"
            )
            if paused:
                session_line += f"\n  ⏸ Paused — resumes in {format_timedelta_until(pause_until)}"

    top3_line = ""
    if s['top3']:
        top3_line = "\n📈 <b>Top Uploaders</b>\n"
        medals = ["🥇", "🥈", "🥉"]
        for i, u in enumerate(s['top3']):
            top3_line += f"  {medals[i]} {u['anonymous_name']} — {u['session_upload_count']} uploads\n"

    refreshed_at = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

    text = (
        "📊 <b>Bot Statistics</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "👥 <b>Users</b>\n"
        f"  Total (All): <b>{total}</b>\n"
        f"  🟢 Active: {s['active']} ({active_pct}%)\n"
        f"  🔴 Inactive: {s['inactive']} | ⏳ Pending: {s['pending']}\n"
        f"  ⛔ Banned: {s['banned']} | 🚫 Blocked bot: {s['blocked_bot']}\n"
        f"  🔐 Unverified: {s['unverified']}\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📈 <b>Activity</b>\n"
        f"  Active last 24h: {s['active_24h']}\n"
        f"  Joined today: {s['joined_today']} | Last 7 days: {s['joined_7d']}\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📤 <b>Media</b>\n"
        f"  In queue: {s['in_queue']} | Sent today: {s['sent_today']}\n"
        f"  This session: {s['session_total']} uploads\n"
        f"  Avg/active user: {s['avg_uploads']}\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"⚡ Total EXP distributed: {s['total_exp']:,}\n"
        f"🚨 Unsolved reports: {s['unsolved_reports']}"
        f"{session_line}"
        f"{top3_line}"
        f"\n\n🕐 <i>Updated: {refreshed_at}</i>"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_stats")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="admin_main")],
    ])
    try:
        await loading.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        try:
            await loading.delete()
        except Exception:
            pass
        await callback.message.answer(text, parse_mode="HTML", reply_markup=kb)


SETTING_LABELS = {
    'broadcast_delay_seconds': ("Broadcast Delay", "seconds", 0, 3600),
    'inactivity_minutes': ("Inactivity Timeout", "minutes", 1, 10080),
    'session_duration_days': ("Session Duration", "days", 1, 365),
    'leaderboard_top': ("Leaderboard Top N", "users", 1, 100),
    'session_pause_hours': ("Session Pause", "hours", 0, 72),
    'activation_threshold': ("Activation Count", "uploads", 1, 100),
    'reactivation_threshold': ("Reactivation Count", "uploads", 1, 50),
}


@router.callback_query(F.data == "admin_settings")
async def admin_settings(callback: CallbackQuery, pool: asyncpg.Pool):
    if not is_admin(callback.from_user.id):
        return
    loading = await _show_loading(callback, "Loading settings")
    await callback.answer()

    try:
        async with asyncio.timeout(5):
            cfg = await get_config(pool)
    except asyncio.TimeoutError:
        await callback.message.edit_text("⚠️ <b>Timeout loading settings.</b>", reply_markup=back_keyboard())
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"📡 Broadcast Delay: {cfg.get('broadcast_delay_seconds', '30')}s",
            callback_data="admin_set_broadcast_delay_seconds"
        )],
        [InlineKeyboardButton(
            text=f"⏱ Inactivity: {cfg.get('inactivity_minutes', '160')}min",
            callback_data="admin_set_inactivity_minutes"
        )],
        [InlineKeyboardButton(
            text=f"📅 Session Duration: {cfg.get('session_duration_days', '7')} days",
            callback_data="admin_set_session_duration_days"
        )],
        [InlineKeyboardButton(
            text=f"🏆 Leaderboard Top: {cfg.get('leaderboard_top', '10')}",
            callback_data="admin_set_leaderboard_top"
        )],
        [InlineKeyboardButton(
            text=f"⏸ Pause After Session: {cfg.get('session_pause_hours', '3')}h",
            callback_data="admin_set_session_pause_hours"
        )],
        [InlineKeyboardButton(
            text=f"🔢 Uploads to Activate: {cfg.get('activation_threshold', '10')}",
            callback_data="admin_set_activation_threshold"
        )],
        [InlineKeyboardButton(
            text=f"🔄 Uploads to Reactivate: {cfg.get('reactivation_threshold', '3')}",
            callback_data="admin_set_reactivation_threshold"
        )],
        [InlineKeyboardButton(text="◀️ Back", callback_data="admin_main")],
    ])
    try:
        await loading.edit_text("⚙️ <b>Settings</b>\n\nTap a setting to change its value.", parse_mode="HTML", reply_markup=kb)
    except Exception:
        try:
            await loading.delete()
        except Exception:
            pass
        await callback.message.answer("⚙️ <b>Settings</b>\n\nTap a setting to change its value.", parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data.startswith("admin_set_"))
async def admin_set_setting(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    key = callback.data.replace("admin_set_", "")
    if key not in SETTING_LABELS:
        await callback.answer("Unknown setting.")
        return

    label, unit, min_val, max_val = SETTING_LABELS[key]
    await state.set_state(AdminSettingState.entering_value)
    await state.update_data(setting_key=key, setting_label=label,
                            setting_unit=unit, min_val=min_val, max_val=max_val)

    await _send_fresh(
        callback,
        f"⚙️ <b>{label}</b>\n\n"
        f"Enter new value ({unit}):\n"
        f"<i>Range: {min_val} – {max_val}</i>\n\n"
        f"Send /cancel to abort."
    )
    await callback.answer()


@router.message(AdminSettingState.entering_value)
async def process_setting_value(message: Message, state: FSMContext, pool: asyncpg.Pool):
    if not is_admin(message.from_user.id):
        return

    if message.text and message.text.strip() == '/cancel':
        await state.clear()
        await message.answer("Cancelled.", reply_markup=admin_main_keyboard())
        return

    data = await state.get_data()
    key = data['setting_key']
    label = data['setting_label']
    unit = data['setting_unit']
    min_val = data['min_val']
    max_val = data['max_val']

    try:
        val = float(message.text.strip())
        if not (min_val <= val <= max_val):
            raise ValueError
    except (ValueError, AttributeError):
        await message.answer(
            f"❌ Invalid. Enter a number between {min_val} and {max_val} ({unit})."
        )
        return

    str_val = str(int(val)) if val == int(val) else str(val)
    await set_config(pool, key, str_val)
    await state.clear()
    await message.answer(
        f"✅ <b>{label}</b> updated to <code>{str_val}</code> {unit}.",
        parse_mode="HTML",
        reply_markup=admin_main_keyboard()
    )


@router.callback_query(F.data.startswith("admin_reports_"))
async def admin_reports(callback: CallbackQuery, pool: asyncpg.Pool):
    if not is_admin(callback.from_user.id):
        return

    try:
        page = int(callback.data.split("_")[-1])
    except ValueError:
        page = 0

    loading = await _show_loading(callback, "Loading reports")
    await callback.answer()

    per_page = 5
    offset = page * per_page

    try:
        async with asyncio.timeout(10):
            reports = await get_unsolved_reports(pool, limit=per_page, offset=offset)
            total = await count_unsolved_reports(pool)
    except asyncio.TimeoutError:
        await callback.message.edit_text("⚠️ <b>Timeout loading reports.</b>", reply_markup=back_keyboard())
        return

    if not reports:
        try:
            await loading.edit_text("🚨 <b>Reports</b>\n\nNo unsolved reports. ✅", parse_mode="HTML", reply_markup=back_keyboard())
        except Exception:
            try:
                await loading.delete()
            except Exception:
                pass
            await callback.message.answer("🚨 <b>Reports</b>\n\nNo unsolved reports. ✅", parse_mode="HTML", reply_markup=back_keyboard())
        return

    buttons = []
    for r in reports:
        ts = r['reported_at'].strftime('%m/%d %H:%M')
        label = f"#{r['id']} — {r['uploader_name'] or 'unknown'} | {r['media_type'] or '?'} | {ts}"
        buttons.append([InlineKeyboardButton(
            text=label[:64],
            callback_data=f"admin_view_report_{r['id']}"
        )])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️ Prev", callback_data=f"admin_reports_{page-1}"))
    if offset + per_page < total:
        nav.append(InlineKeyboardButton(text="Next ▶️", callback_data=f"admin_reports_{page+1}"))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="◀️ Back", callback_data="admin_main")])

    text = f"🚨 <b>Unsolved Reports</b> ({total} total) — Page {page + 1}:"
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    try:
        await loading.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        try:
            await loading.delete()
        except Exception:
            pass
        await callback.message.answer(text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data.startswith("admin_view_report_"))
async def admin_view_report(callback: CallbackQuery, pool: asyncpg.Pool, bot: Bot):
    if not is_admin(callback.from_user.id):
        return

    report_id = int(callback.data.split("_")[-1])

    async with pool.acquire() as conn:
        report = await conn.fetchrow("SELECT * FROM reports WHERE id = $1", report_id)

    if not report:
        await callback.answer("Report not found.")
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Mark Solved", callback_data=f"solve_report_{report_id}"),
            InlineKeyboardButton(
                text="🚫 Ban Uploader",
                callback_data=f"admin_ban_{report['uploader_id']}"
            ),
        ],
        [InlineKeyboardButton(text="◀️ Back", callback_data="admin_reports_0")],
    ])

    status = '✅ Solved' if report['solved'] else '🔴 Unsolved'
    text = (
        f"🚨 <b>Report #{report_id}</b>\n\n"
        f"Uploader: <b>{report['uploader_name'] or 'unknown'}</b>\n"
        f"Media type: {report['media_type'] or '—'}\n"
        f"Reported at: {format_datetime(report['reported_at'])}\n"
        f"Status: {status}"
    )

    try:
        await callback.message.delete()
    except Exception:
        pass

    if report['media_file_id'] and report['media_type']:
        try:
            if report['media_type'] == 'photo':
                await bot.send_photo(callback.from_user.id, report['media_file_id'],
                                     caption=text, parse_mode="HTML", reply_markup=kb)
            elif report['media_type'] == 'video':
                await bot.send_video(callback.from_user.id, report['media_file_id'],
                                     caption=text, parse_mode="HTML", reply_markup=kb)
            else:
                await bot.send_document(callback.from_user.id, report['media_file_id'],
                                        caption=text, parse_mode="HTML", reply_markup=kb)
            await callback.answer()
            return
        except Exception:
            pass

    await callback.message.answer(text, parse_mode="HTML", reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("solve_report_"))
async def cb_solve_report(callback: CallbackQuery, pool: asyncpg.Pool):
    if not is_admin(callback.from_user.id):
        return
    report_id = int(callback.data.split("_")[-1])
    await solve_report(pool, report_id)
    await callback.answer("✅ Marked as solved.")
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass


@router.callback_query(F.data == "admin_users")
async def admin_users(callback: CallbackQuery, state: FSMContext, pool: asyncpg.Pool):
    if not is_admin(callback.from_user.id):
        return
    loading = await _show_loading(callback, "Loading users")
    await callback.answer()

    try:
        async with asyncio.timeout(12):
            stats = await get_advanced_stats(pool)
    except asyncio.TimeoutError:
        await callback.message.edit_text("⚠️ <b>Timeout loading user stats.</b>", reply_markup=back_keyboard())
        return

    if stats.get('status') != 'ok':
        await callback.message.edit_text("❌ <b>Error loading user stats.</b>", reply_markup=back_keyboard())
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Search by Name or ID", callback_data="admin_users_search")],
        [InlineKeyboardButton(text="♻️ Reset Blocked Status", callback_data="admin_users_reset_blocked")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="admin_main")],
    ])
    text = (
        f"👥 <b>User Management</b>\n\n"
        f"Total: <b>{stats['total']}</b>\n"
        f"🟢 Active: {stats['active']} | 🔴 Inactive: {stats['inactive']}\n"
        f"⏳ Pending: {stats['pending']} | ⛔ Banned: {stats['banned']}\n"
        f"🚫 Blocked bot: <b>{stats['blocked_bot']}</b>\n\n"
        f"Search by anonymous name or Telegram ID."
    )
    try:
        await loading.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        try:
            await loading.delete()
        except Exception:
            pass
        await callback.message.answer(text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data == "admin_users_reset_blocked")
async def admin_users_reset_blocked(callback: CallbackQuery, pool: asyncpg.Pool):
    if not is_admin(callback.from_user.id):
        return
    
    count = await reset_all_blocked_status(pool)
    await callback.answer(f"✅ Reset blocked status for {count} users.", show_alert=True)
    await admin_users(callback, None, pool)


@router.callback_query(F.data == "admin_users_search")
async def admin_users_search(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminUserState.searching)
    await _send_fresh(
        callback,
        "🔍 Enter anonymous name or Telegram ID:\n\nSend /cancel to abort."
    )
    await callback.answer()


@router.message(AdminUserState.searching)
async def process_user_search(message: Message, state: FSMContext, pool: asyncpg.Pool):
    if not is_admin(message.from_user.id):
        return

    if message.text and message.text.strip() == '/cancel':
        await state.clear()
        await message.answer("Cancelled.", reply_markup=admin_main_keyboard())
        return

    query = message.text.strip()
    user = await get_user_by_id_or_name(pool, query)
    await state.clear()

    if not user:
        await message.answer(
            f"❌ No user found: <code>{query}</code>",
            parse_mode="HTML",
            reply_markup=admin_main_keyboard()
        )
        return

    await show_user_detail(message, user, pool)


async def show_user_detail(target, user, pool):
    action_btn = (
        InlineKeyboardButton(text="✅ Unban", callback_data=f"admin_unban_{user['id']}")
        if user['status'] == 'banned'
        else InlineKeyboardButton(text="🚫 Ban", callback_data=f"admin_ban_{user['id']}")
    )

    status_icon = {
        'active': '🟢', 'inactive': '🔴', 'pending': '⏳', 'banned': '⛔'
    }.get(user['status'], '❓')

    badges = get_all_badges(user['badge_emoji'])
    blocked_str = " 🚫 Has blocked bot" if user.get('bot_blocked') else ""

    text = (
        f"👤 <b>{user['anonymous_name']}</b>\n"
        f"   ID: <code>{user['id']}</code>{blocked_str}\n\n"
        f"Status: {status_icon} {user['status'].capitalize()}\n"
        f"⚡ Level {user['level']} — {user['exp']} EXP\n"
        f"📤 Total media: {user['total_media_lifetime']}\n"
        f"📊 Session uploads: {user['session_upload_count']}\n"
        f"🏅 Badges: {badges}\n"
        f"📅 Joined: {format_datetime(user['joined_at'])}\n"
        f"🕐 Last active: {format_datetime(user['last_activity_at'])}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [action_btn],
        [InlineKeyboardButton(text="◀️ Back", callback_data="admin_users")],
    ])
    await target.answer(text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data.startswith("admin_ban_"))
async def cb_ban_user(callback: CallbackQuery, pool: asyncpg.Pool, bot: Bot):
    if not is_admin(callback.from_user.id):
        return
    user_id = int(callback.data.split("_")[-1])
    await ban_user(pool, user_id)
    try:
        await bot.send_message(user_id, "🚫 You have been banned from this bot.")
    except Exception:
        pass
    
    # If the message being interacted with is a report view, update its status text
    if callback.message.caption and "🚨 Report #" in callback.message.caption:
        new_caption = callback.message.caption.replace("Status: 🔴 Unsolved", "Status: ✅ Solved (Banned)")
        try:
            await callback.message.edit_caption(caption=new_caption, parse_mode="HTML")
        except Exception:
            pass
    elif callback.message.text and "🚨 Report #" in callback.message.text:
        new_text = callback.message.text.replace("Status: 🔴 Unsolved", "Status: ✅ Solved (Banned)")
        try:
            await callback.message.edit_text(text=new_text, parse_mode="HTML")
        except Exception:
            pass

    await callback.answer("🚫 User banned.")
    try:
        await callback.message.edit_reply_markup(
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Unban", callback_data=f"admin_unban_{user_id}")],
                [InlineKeyboardButton(text="◀️ Back", callback_data="admin_users")],
            ])
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("admin_unban_"))
async def cb_unban_user(callback: CallbackQuery, pool: asyncpg.Pool, bot: Bot):
    if not is_admin(callback.from_user.id):
        return
    user_id = int(callback.data.split("_")[-1])
    await unban_user(pool, user_id)
    try:
        await bot.send_message(user_id, "✅ You have been unbanned. Use /start to continue.")
    except Exception:
        pass
    await callback.answer("✅ User unbanned.")
    try:
        await callback.message.edit_reply_markup(
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🚫 Ban", callback_data=f"admin_ban_{user_id}")],
                [InlineKeyboardButton(text="◀️ Back", callback_data="admin_users")],
            ])
        )
    except Exception:
        pass


@router.callback_query(F.data == "admin_announce")
async def admin_announce(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.clear()
    await state.update_data(pin_message=False)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📌 Pin: OFF", callback_data="admin_announce_toggle_pin")],
        [InlineKeyboardButton(text="✏️ Compose Message", callback_data="admin_announce_compose")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="admin_main")],
    ])
    await _send_fresh(
        callback,
        "📢 <b>Broadcast Announcement</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "Send any content to all non-banned users.\n"
        "Supports: text, photo, video, document, sticker, voice, audio, animation.\n\n"
        "📌 <b>Pin:</b> OFF\n\n"
        "Toggle pin, then compose your message.",
        reply_markup=kb
    )
    await callback.answer()


@router.callback_query(F.data == "admin_announce_toggle_pin")
async def admin_announce_toggle_pin(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    data = await state.get_data()
    new_pin = not data.get('pin_message', False)
    await state.update_data(pin_message=new_pin)

    pin_label = "ON ✅" if new_pin else "OFF"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📌 Pin: {pin_label}", callback_data="admin_announce_toggle_pin")],
        [InlineKeyboardButton(text="✏️ Compose Message", callback_data="admin_announce_compose")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="admin_main")],
    ])
    pin_desc = "Message will be <b>pinned</b> in each user's chat." if new_pin else "Message will be sent normally (not pinned)."
    await _send_fresh(
        callback,
        "📢 <b>Broadcast Announcement</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "Send any content to all non-banned users.\n"
        "Supports: text, photo, video, document, sticker, voice, audio, animation.\n\n"
        f"📌 <b>Pin:</b> {pin_label}\n"
        f"{pin_desc}\n\n"
        "Toggle pin, then compose your message.",
        reply_markup=kb
    )
    await callback.answer()


@router.callback_query(F.data == "admin_announce_compose")
async def admin_announce_compose(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    data = await state.get_data()
    pin = data.get('pin_message', False)
    pin_label = "ON ✅" if pin else "OFF"

    await state.set_state(AdminAnnounceState.entering_message)
    await _send_fresh(
        callback,
        f"✏️ <b>Compose your broadcast</b>\n\n"
        f"Send any message now — text, photo, video, document, sticker, voice, audio, or animation.\n\n"
        f"📌 Pin: <b>{pin_label}</b>\n\n"
        f"Send /cancel to abort."
    )
    await callback.answer()


@router.message(AdminAnnounceState.entering_message)
async def process_announcement_preview(message: Message, state: FSMContext, pool: asyncpg.Pool, bot: Bot):
    if not is_admin(message.from_user.id):
        return

    if message.text and message.text.strip() == '/cancel':
        await state.clear()
        await message.answer("Cancelled.", reply_markup=admin_main_keyboard())
        return

    from database import get_all_notifiable_users
    users = await get_all_notifiable_users(pool)
    user_count = len(users)

    data = await state.get_data()
    pin = data.get('pin_message', False)
    pin_label = "ON ✅" if pin else "OFF"

    await state.set_state(AdminAnnounceState.confirming)
    await state.update_data(
        announce_msg_id=message.message_id,
        announce_chat_id=message.chat.id,
        announce_user_count=user_count
    )

    content_type = "text"
    if message.photo:
        content_type = "📷 Photo"
    elif message.video:
        content_type = "🎬 Video"
    elif message.document:
        content_type = "📎 Document"
    elif message.sticker:
        content_type = "🏷 Sticker"
    elif message.voice:
        content_type = "🎤 Voice"
    elif message.audio:
        content_type = "🎵 Audio"
    elif message.animation:
        content_type = "🎞 Animation"
    elif message.text:
        preview_text = message.text[:100]
        if len(message.text) > 100:
            preview_text += "..."
        content_type = f"📝 Text: <i>{preview_text}</i>"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"✅ Send to {user_count} users", callback_data="admin_announce_confirm"),
            InlineKeyboardButton(text="❌ Cancel", callback_data="admin_announce_cancel"),
        ],
    ])

    await message.answer(
        "📢 <b>Confirm Broadcast</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        f"Content: {content_type}\n"
        f"📌 Pin: <b>{pin_label}</b>\n"
        f"👥 Recipients: <b>{user_count}</b> users\n\n"
        f"⚠️ <b>Are you sure you want to send this?</b>",
        parse_mode="HTML",
        reply_markup=kb
    )


@router.callback_query(F.data == "admin_announce_cancel")
async def admin_announce_cancel(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.clear()
    await _send_fresh(callback, "❌ Broadcast cancelled.", reply_markup=admin_main_keyboard())
    await callback.answer()


@router.callback_query(F.data == "admin_announce_confirm")
async def admin_announce_confirmed(callback: CallbackQuery, state: FSMContext, pool: asyncpg.Pool, bot: Bot):
    if not is_admin(callback.from_user.id):
        return

    data = await state.get_data()
    msg_id = data.get('announce_msg_id')
    chat_id = data.get('announce_chat_id')
    pin_message = data.get('pin_message', False)
    total_users = data.get('announce_user_count', 0)

    if not msg_id or not chat_id:
        await state.clear()
        await callback.answer("⚠️ Session expired, please try again.")
        return

    await state.clear()

    from database import get_all_notifiable_users, mark_user_blocked
    users = await get_all_notifiable_users(pool)
    total_users = len(users)

    pin_label = " + 📌 Pin" if pin_message else ""
    try:
        await callback.message.delete()
    except Exception:
        pass

    from utils.limiter import global_rate_limiter
    from tasks.cleanup import generate_progress_bar
    status_msg = await callback.message.answer(
        f"📢 <b>Broadcasting{pin_label}...</b>\n\n"
        f"⏳ Progress: 0/{total_users} (0%)\n"
        f"<code>{generate_progress_bar(0)}</code>",
        parse_mode="HTML"
    )

    sent = 0
    failed = 0
    blocked = 0
    pinned = 0
    last_update = -1

    for i, user in enumerate(users, 1):
        # Rate limiting
        await global_rate_limiter.consume()

        try:
            copied = await bot.copy_message(
                chat_id=user['id'],
                from_chat_id=chat_id,
                message_id=msg_id
            )
            sent += 1
            if pin_message:
                try:
                    await bot.pin_chat_message(
                        chat_id=user['id'],
                        message_id=copied.message_id,
                        disable_notification=False
                    )
                    pinned += 1
                except Exception as pin_err:
                    logger.debug(f"Pin failed for {user['id']}: {pin_err}")
        except TelegramForbiddenError as e:
            logger.info(f"Broadcast: User {user['id']} blocked the bot (403). Marking as blocked.")
            await mark_user_blocked(pool, user['id'])
            blocked += 1
        except Exception as e:
            err = str(e).lower()
            # Only mark as blocked for very specific Telegram errors that mean the chat is gone
            if any(x in err for x in ['chat not found', 'user_deactivated']):
                logger.info(f"Broadcast: User {user['id']} unavailable ({err}). Marking as blocked.")
                await mark_user_blocked(pool, user['id'])
                blocked += 1
            else:
                logger.error(f"Broadcast: Failed to send to {user['id']}: {e}")
                failed += 1

        # Jittered delay
        await asyncio.sleep(0.02 * random.uniform(0.8, 1.2))

        pct = int(i / total_users * 100)
        if pct > last_update or i == total_users:
            last_update = pct
            bar = generate_progress_bar(pct)
            try:
                await status_msg.edit_text(
                    f"📢 <b>Broadcasting{pin_label}...</b>\n\n"
                    f"⏳ Progress: {i}/{total_users} ({pct}%)\n"
                    f"<code>{bar}</code>\n\n"
                    f"✅ Sent: {sent} | 🚫 Blocked: {blocked} | ❌ Failed: {failed}",
                    parse_mode="HTML"
                )
            except TelegramBadRequest:
                pass

    try:
        await status_msg.delete()
    except Exception:
        pass

    pin_report = f"\n📌 Pinned: <b>{pinned}</b>" if pin_message else ""
    await callback.message.answer(
        f"✅ <b>Broadcast Complete!</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"📨 Sent: <b>{sent}</b>{pin_report}\n"
        f"🚫 Blocked bot: <b>{blocked}</b>\n"
        f"❌ Failed: <b>{failed}</b>\n"
        f"👥 Total recipients: <b>{total_users}</b>",
        parse_mode="HTML",
        reply_markup=admin_main_keyboard()
    )


@router.callback_query(F.data == "admin_session")
async def admin_session_menu(callback: CallbackQuery, pool: asyncpg.Pool):
    if not is_admin(callback.from_user.id):
        return

    loading = await _show_loading(callback, "Loading session")
    await callback.answer()

    try:
        async with asyncio.timeout(10):
            stats = await get_session_stats(pool)
            session = stats['session']
            config = await get_config(pool)
    except asyncio.TimeoutError:
        await callback.message.edit_text("⚠️ <b>Timeout loading session data.</b>", reply_markup=back_keyboard())
        return

    paused, pause_until = await is_session_paused(pool)

    text = f"🏆 <b>Session #{session.get('session_number', '?')}</b>\n\n"

    started = session.get('started_at')
    if started:
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        running_for = now - started
        text += f"Started: {format_datetime(started)} ({running_for.days}d ago)\n"

        duration_days = int(config.get('session_duration_days', '7'))
        session_end = started + timedelta(days=duration_days)
        text += f"Scheduled end: {format_datetime(session_end)}\n"

    text += f"Total uploads: <b>{stats['total_uploads']}</b>\n"

    if stats['top_user']:
        text += (
            f"🥇 Leader: <b>{stats['top_user']['anonymous_name']}</b> "
            f"({stats['top_user']['session_upload_count']} uploads)\n"
        )

    if paused:
        text += f"\n⏸ <b>Currently Paused</b> — resumes in {format_timedelta_until(pause_until)}"

    pause_hours = config.get('session_pause_hours', '3')
    text += f"\n⏸ Pause after end: {pause_hours}h (media cleanup period)"

    refreshed_at = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    text += f"\n\n🕐 <i>Updated: {refreshed_at}</i>"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_session")],
        [InlineKeyboardButton(text="⏹ End Session Now", callback_data="admin_session_confirm")],
        [InlineKeyboardButton(text="◀️ Back", callback_data="admin_main")],
    ])
    try:
        await loading.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        try:
            await loading.delete()
        except Exception:
            pass
        await callback.message.answer(text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data == "admin_session_confirm")
async def admin_session_confirm(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Yes, End It", callback_data="admin_session_end"),
            InlineKeyboardButton(text="❌ Cancel", callback_data="admin_session"),
        ]
    ])
    await _send_fresh(
        callback,
        "⚠️ <b>Confirm Session End</b>\n\n"
        "This will:\n"
        "• Assign permanent badges to top uploaders\n"
        "• Delete ALL media from this session\n"
        "• Reset all session upload counts to 0\n"
        "• Pause all uploads for the cleanup period\n"
        "• Announce results to all users\n"
        "• Start a new session\n\n"
        "<b>This cannot be undone.</b>",
        reply_markup=kb
    )
    await callback.answer()


@router.callback_query(F.data == "admin_session_end")
async def admin_do_end_session(callback: CallbackQuery, pool: asyncpg.Pool, bot: Bot):
    if not is_admin(callback.from_user.id):
        return

    await callback.answer("⏳ Processing...")

    try:
        await callback.message.delete()
    except Exception:
        pass
    
    from tasks.cleanup import generate_progress_bar
    
    progress_msg = await callback.message.answer(
        "🏆 <b>Ending Session</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "⏳ Initializing...\n"
        f"<code>{generate_progress_bar(0)}</code> 0%",
        parse_mode="HTML"
    )

    async def update_progress(label: str, pct: int):
        bar = generate_progress_bar(pct)
        try:
            await progress_msg.edit_text(
                f"🏆 <b>Ending Session</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n\n"
                f"⏳ {label}\n"
                f"<code>{bar}</code> {pct}%\n",
                parse_mode="HTML"
            )
        except TelegramBadRequest:
            pass # Message not modified or too frequent updates
        except Exception as e:
            logger.debug(f"Progress update error: {e}")

    config = await get_config(pool)
    pause_hours = float(config.get('session_pause_hours', '3'))
    top_n = int(config.get('leaderboard_top', '10'))

    session = await get_current_session(pool)
    if not session:
        try:
            await progress_msg.delete()
        except Exception:
            pass
        await callback.message.answer("❌ No active session found.", reply_markup=admin_main_keyboard())
        return

    # Pass the progress callback to end_session
    result = await end_session(pool, session['id'], pause_hours, top_n, progress_callback=update_progress)
    
    if not result:
        try:
            await progress_msg.delete()
        except Exception:
            pass
        await callback.message.answer(
            "⚠️ This session has already been ended.",
            parse_mode="HTML",
            reply_markup=admin_main_keyboard()
        )
        return

    try:
        await update_progress("Broadcasting results to users...", 98)
    except Exception:
        pass

    await broadcast_session_results(bot, pool, result, pause_hours)
    asyncio.get_running_loop().create_task(
        delete_session_messages(bot, pool, session['id'])
    )

    badges_count = len(result['badge_assignments'])
    pause_until = result['pause_until']
    from utils.helpers import format_timedelta_until
    new_session_in = format_timedelta_until(pause_until)

    try:
        await progress_msg.delete()
    except Exception:
        pass

    await callback.message.answer(
        f"✅ <b>Session #{session['session_number']} ended successfully!</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"• Badges assigned: <b>{badges_count}</b>\n"
        f"• All session media deleted\n"
        f"• Leaderboard reset\n"
        f"• Uploads paused for <b>{pause_hours}h</b> (cleanup period)\n\n"
        f"⏳ New session starts automatically in <b>{new_session_in}</b>.",
        parse_mode="HTML",
        reply_markup=admin_main_keyboard()
    )


@router.callback_query(F.data == "admin_emergency_wipe")
async def admin_emergency_wipe(callback: CallbackQuery, pool: asyncpg.Pool):
    if not is_admin(callback.from_user.id):
        return

    loading = await _show_loading(callback, "Analyzing media data")
    await callback.answer()

    try:
        async with asyncio.timeout(12): # slightly longer than db timeout
            stats = await get_wipe_stats(pool)
    except asyncio.TimeoutError:
        await loading.edit_text(
            "⚠️ <b>Analysis Timeout</b>\n\nThe media analysis took too long. Please try again.",
            reply_markup=back_keyboard()
        )
        return

    if stats.get('status') != 'ok':
        error_msg = stats.get('error', 'Unknown error')
        await loading.edit_text(
            f"❌ <b>Error Analyzing Media</b>\n\n<code>{error_msg}</code>",
            reply_markup=back_keyboard()
        )
        return

    paused, pause_until = await is_session_paused(pool)

    pause_line = ""
    if paused:
        pause_line = f"\n⏸ Session paused — resumes in {format_timedelta_until(pause_until)}\n"

    if stats['total_messages'] == 0:
        text = (
            "🚨 <b>Emergency Wipe</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            "✅ No tracked messages to wipe.\n"
            f"{pause_line}\n"
            "There are no media messages in user chats."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Back", callback_data="admin_main")],
        ])
    else:
        text = (
            "🚨 <b>Emergency Wipe</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            "<b>📊 Media Stats:</b>\n"
            f"  📨 Messages to wipe: <b>{stats['total_messages']:,}</b>\n"
            f"  👥 Users affected: <b>{stats['unique_recipients']:,}</b>\n"
            f"  📁 Across sessions: <b>{stats['unique_sessions']}</b>\n"
            f"  📤 In broadcast queue: <b>{stats['media_in_queue']}</b>\n"
            f"{pause_line}\n"
            "<b>👥 User Status:</b>\n"
            f"  🟢 Active users: <b>{stats['active_users']}</b>\n"
            f"  📬 Notifiable users: <b>{stats['total_users']}</b>\n\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "⚠️ This will <b>delete ALL tracked media</b> from every user's chat "
            "and notify everyone with a live progress bar.\n\n"
            "<b>This cannot be undone.</b>"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ YES — Wipe Everything", callback_data="admin_emergency_wipe_confirm"),
                InlineKeyboardButton(text="❌ NO — Cancel", callback_data="admin_main"),
            ],
            [InlineKeyboardButton(text="◀️ Back", callback_data="admin_main")],
        ])

    try:
        await loading.edit_text(text, parse_mode="HTML", reply_markup=kb)
    except Exception:
        try:
            await loading.delete()
        except Exception:
            pass
        await callback.message.answer(text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data == "admin_emergency_wipe_confirm")
async def admin_emergency_wipe_confirm(callback: CallbackQuery, pool: asyncpg.Pool, bot: Bot):
    if not is_admin(callback.from_user.id):
        return

    await callback.answer("🚨 Starting emergency wipe...")

    try:
        await callback.message.delete()
    except Exception:
        pass

    progress_msg = await callback.message.answer(
        "🚨 <b>Emergency Wipe Starting...</b>\n\n"
        "⏳ Preparing to delete all tracked media...",
        parse_mode="HTML"
    )

    result = await emergency_wipe_all(bot, pool, admin_msg=progress_msg)

    admin_done = (
        f"🚨 <b>Emergency Wipe Complete</b>\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"🗑 Deleted: <b>{result['deleted']:,}</b>\n"
        f"⏭ Skipped: <b>{result['skipped']:,}</b>\n"
        f"📊 Total processed: <b>{result['total']:,}</b>\n"
        f"⏱ Time: <b>{result['elapsed']}s</b>\n\n"
        f"✅ All user chats have been cleaned."
    )

    try:
        await progress_msg.edit_text(admin_done, parse_mode="HTML", reply_markup=admin_main_keyboard())
    except Exception:
        try:
            await progress_msg.delete()
        except Exception:
            pass
        await callback.message.answer(admin_done, parse_mode="HTML", reply_markup=admin_main_keyboard())


@router.callback_query(F.data.startswith("report_"))
async def cb_report_media(callback: CallbackQuery, pool: asyncpg.Pool, bot: Bot):
    parts = callback.data.split("_")
    if len(parts) < 2:
        await callback.answer("Invalid report.")
        return

    try:
        media_id = int(parts[1])
    except ValueError:
        await callback.answer("Invalid report.")
        return

    reporter_id = callback.from_user.id
    reporter = await get_user(pool, reporter_id)
    if not reporter:
        await callback.answer("You must be registered to report.")
        return

    if reporter['status'] == 'banned':
        await callback.answer("You are banned.")
        return

    async with pool.acquire() as conn:
        existing = await conn.fetchval(
            "SELECT 1 FROM reports WHERE reporter_id = $1 AND media_id = $2",
            reporter_id, media_id
        )
        if existing:
            await callback.answer("⚠️ You already reported this media.")
            return

        media = await conn.fetchrow("SELECT * FROM media WHERE id = $1", media_id)

    if not media:
        await callback.answer("⚠️ This media is no longer available.")
        return

    uploader = await get_user(pool, media['user_id'])
    uploader_name = uploader['anonymous_name'] if uploader else "unknown"

    report = await create_report(
        pool, reporter_id, media_id,
        media['user_id'], uploader_name,
        media['file_id'], media['media_type']
    )

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
        f"Reporter: <b>{reporter['anonymous_name']}</b>\n"
        f"Media ID: {media_id} | Type: {media['media_type']}"
    )

    try:
        if media['media_type'] == 'photo':
            msg = await bot.send_photo(ADMIN_ID, media['file_id'],
                                       caption=caption, parse_mode="HTML", reply_markup=kb)
        elif media['media_type'] == 'video':
            msg = await bot.send_video(ADMIN_ID, media['file_id'],
                                       caption=caption, parse_mode="HTML", reply_markup=kb)
        else:
            msg = await bot.send_document(ADMIN_ID, media['file_id'],
                                          caption=caption, parse_mode="HTML", reply_markup=kb)
        await set_report_admin_message(pool, report['id'], msg.message_id)
    except Exception as e:
        logger.error(f"Failed to send report to admin: {e}")

    await callback.answer("🚨 Report submitted. Thank you.")
