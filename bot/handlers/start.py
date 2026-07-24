import random
import logging
import time
import asyncio
import string
from aiogram import Router, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

import asyncpg
from database import (
    get_user, create_user, name_exists, get_config, mark_user_unblocked,
    save_pending_verification, get_pending_verification, clear_pending_verification,
    is_session_paused, get_start_context, get_verification_context,
    set_user_opted_out, get_invite_key, set_referral_code,
    get_user_by_referral_code, set_referred_by, get_referral_count
)
from utils.names import generate_anonymous_name
from utils.helpers import format_timedelta_until, safe_error
from utils.levels import referral_badge_for_count, referral_exp_bonus, REFERRAL_BADGES

logger = logging.getLogger(__name__)
router = Router()

_start_cooldowns: dict[int, float] = {}
START_COOLDOWN = 5
COOLDOWN_TTL = 600
BOT_USERNAME = None

DISCLAIMER_TEXT = (
    "🌐 <b>Disclaimer</b>\n\n"
    "This service may contain content that is sensitive or not suitable for all audiences. "
    "Viewer discretion is advised.\n\n"
    "By continuing, you acknowledge that you may encounter content including strong language, "
    "adult themes, or other potentially disturbing material.\n\n"
    "Do you wish to continue?"
)

TERMS_TEXT = (
    "📜 <b>Terms of Use</b>\n\n"
    "By using this bot, you agree to the following:\n\n"
    "• Content you upload <b>will be shared with other active users</b> automatically\n"
    "• You may <b>receive content from other users</b> automatically\n"
    "• 🛡️ <b>Active Moderation</b>: All uploaded media is actively scanned and monitored. Uploading illegal or prohibited content will result in an immediate, permanent ban.\n"
    "• You are responsible for anything you upload — <b>no harmful material</b>\n\n"
    "Controls available to you:\n"
    "• /stop — Stop receiving content at any time\n"
    "• /start — Resume participation\n"
    "• /report — Flag inappropriate content\n\n"
    "Failure to follow these rules may result in restrictions or removal from the service.\n\n"
    "By continuing, you confirm that you understand and accept these terms."
)


def _cleanup_cooldowns():
    now = time.time()
    expired = [k for k, v in _start_cooldowns.items() if now - v > COOLDOWN_TTL]
    for k in expired:
        del _start_cooldowns[k]


def _generate_referral_code() -> str:
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))


class OnboardingState(StatesGroup):
    disclaimer = State()
    terms = State()


class VerificationState(StatesGroup):
    waiting_answer = State()


def make_math_question() -> tuple[str, int]:
    ops = ['+', '-', '*']
    op = random.choice(ops)
    if op == '+':
        a, b = random.randint(5, 30), random.randint(5, 30)
        answer = a + b
    elif op == '-':
        a = random.randint(15, 50)
        b = random.randint(1, a)
        answer = a - b
    else:
        a, b = random.randint(2, 12), random.randint(2, 12)
        answer = a * b
    return f"{a} {op} {b} = ?", answer


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, pool: asyncpg.Pool):
    global BOT_USERNAME
    user_id = message.from_user.id

    now = time.time()
    _cleanup_cooldowns()
    if now - _start_cooldowns.get(user_id, 0) < START_COOLDOWN:
        return
    _start_cooldowns[user_id] = now

    # Cache bot username
    if BOT_USERNAME is None:
        try:
            bot_info = await message.bot.get_me()
            BOT_USERNAME = bot_info.username
        except Exception:
            BOT_USERNAME = "Aegis10Gatebot"

    # Parse deeplink payload
    args = message.text.split(maxsplit=1)[1].strip() if len(message.text.split()) > 1 else ""

    # 1. Fetch User, Pending, Session, and Config in ONE optimized call
    context = await get_start_context(pool, user_id)
    if not context['success']:
        await message.answer("⚠️ Database is busy. Please try again in a moment.")
        return

    user = context['user']
    pending = context['pending']
    session = context['session']
    config = context['config']

    if user:
        if user['status'] == 'banned':
            await message.answer("🚫 You are permanently banned from this bot.")
            return

        await mark_user_unblocked(pool, user_id)

        # Handle opted-out users — offer to resume
        if user.get('opted_out'):
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Resume", callback_data="resume_opted_in"),
                    InlineKeyboardButton(text="❌ Stay opted out", callback_data="stay_opted_out"),
                ]
            ])
            await message.answer(
                f"👋 Welcome back, <b>{user['anonymous_name']}</b>!\n\n"
                f"You were opted out of receiving content.\n\n"
                f"Would you like to resume?",
                parse_mode="HTML",
                reply_markup=kb
            )
            await state.clear()
            return

        # 2. Check Session Status locally using fetched session
        paused = False
        pause_until = None
        if session and session['pause_until']:
            from datetime import datetime, timezone
            now_dt = datetime.now(timezone.utc)
            pause_until = session['pause_until']
            if pause_until.tzinfo is None:
                pause_until = pause_until.replace(tzinfo=timezone.utc)
            if now_dt < pause_until:
                paused = True

        pause_note = ""
        if paused:
            time_left = format_timedelta_until(pause_until)
            pause_note = (
                f"\n\n⏸ <b>Session is transitioning.</b>\n"
                f"Uploads resume in <b>{time_left}</b>."
            )

        reactivation_threshold = int(config.get('reactivation_threshold', '3'))
        activation_threshold = int(config.get('activation_threshold', '10'))

        status_map = {
            'pending': (
                f"⏳ You're registered as <b>{user['anonymous_name']}</b>.\n"
                f"Upload <b>{activation_threshold}</b> media files to activate your account."
                f"{pause_note}"
            ),
            'active': (
                f"✅ You're active as <b>{user['anonymous_name']}</b>.\n"
                f"Use /me to see your stats."
                f"{pause_note}"
            ),
            'inactive': (
                f"💤 You're registered as <b>{user['anonymous_name']}</b> (inactive).\n"
                f"Upload <b>{reactivation_threshold}</b> media file(s) to reactivate."
                f"{pause_note}"
            ),
        }
        await message.answer(status_map.get(user['status'], "Welcome back."), parse_mode="HTML")
        await state.clear()
        return

    # --- NEW USER FLOW ---

    # Invite key gate: new users MUST provide a valid key via deeplink
    invite_key = await get_invite_key(pool)

    if not args:
        await message.answer(
            "🔒 <b>This bot is invite-only</b>\n\n"
            "You need an invite link to join.\n"
            "Ask someone who's already in to share their link!",
            parse_mode="HTML"
        )
        return

    # Parse key and optional referral code from deeplink
    # Format: invite_key  or  invite_key--REFERRALCODE
    # Using -- as separator since _ can appear in token_urlsafe invite keys
    if "--" in args:
        parts = args.split("--", 1)
        provided_key = parts[0]
        referral_code = parts[1] if len(parts) > 1 else None
    else:
        provided_key = args
        referral_code = None

    if provided_key != invite_key:
        await message.answer(
            "❌ <b>Invalid invite key</b>\n\n"
            "The link you used is not valid. Please get a fresh invite link.",
            parse_mode="HTML"
        )
        return

    # Store referral info in DB (pending_verifications) for survival across FSM loss
    await state.update_data(referral_code=referral_code)

    if pending:
        # User already passed disclaimer/terms but is mid-verification
        reserved_name = pending['reserved_name']
        question, new_answer = make_math_question()
        await save_pending_verification(pool, user_id, new_answer, reserved_name, referral_code=referral_code)
        await state.set_state(VerificationState.waiting_answer)
        await state.update_data(answer=new_answer, referral_code=referral_code)
        await message.answer(
            "🔐 <b>Verification Required</b>\n\n"
            f"Solve this to continue:\n\n"
            f"<code>{question}</code>\n\n"
            "Reply with the number only.",
            parse_mode="HTML"
        )
        return

    # Brand new user with valid key — show disclaimer first
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Yes, Continue", callback_data="disclaimer_accept"),
            InlineKeyboardButton(text="❌ No, Exit", callback_data="disclaimer_reject"),
        ]
    ])
    await message.answer(DISCLAIMER_TEXT, parse_mode="HTML", reply_markup=kb)
    await state.set_state(OnboardingState.disclaimer)


@router.callback_query(F.data == "disclaimer_accept", OnboardingState.disclaimer)
async def disclaimer_accept(callback: CallbackQuery, state: FSMContext, pool: asyncpg.Pool):
    if callback.from_user.id != callback.message.chat.id:
        await callback.answer()
        return

    await callback.answer()

    # Show terms with accept button
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ I Accept", callback_data="terms_accept")]
    ])
    terms_msg = await callback.message.edit_text(TERMS_TEXT, parse_mode="HTML", reply_markup=kb)

    # Try to pin the terms message
    try:
        from utils.limiter import global_rate_limiter
        await global_rate_limiter.consume_for_user(callback.message.chat.id, priority=True)
        bot = callback.bot
        await bot.pin_chat_message(callback.message.chat.id, terms_msg.message_id, disable_notification=True)
    except Exception:
        pass

    await state.set_state(OnboardingState.terms)


@router.callback_query(F.data == "disclaimer_reject", OnboardingState.disclaimer)
async def disclaimer_reject(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != callback.message.chat.id:
        await callback.answer()
        return

    await callback.answer()
    await callback.message.edit_text(
        "No problem! You can /start anytime if you change your mind.",
        parse_mode="HTML"
    )
    await state.clear()


@router.callback_query(F.data == "terms_accept", OnboardingState.terms)
async def terms_accept(callback: CallbackQuery, state: FSMContext, pool: asyncpg.Pool):
    if callback.from_user.id != callback.message.chat.id:
        await callback.answer()
        return

    await callback.answer()

    user_id = callback.from_user.id

    # Remove the "I Accept" button from the pinned terms message (keep it pinned)
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    # Send verification as a NEW message below the pinned terms
    name = generate_anonymous_name()
    attempts = 0
    while await name_exists(pool, name) and attempts < 20:
        name = generate_anonymous_name()
        attempts += 1

    question, answer = make_math_question()
    # Preserve referral_code from FSM and store in DB
    existing_data = await state.get_data()
    ref_code_fsm = existing_data.get('referral_code')
    await save_pending_verification(pool, user_id, answer, name, referral_code=ref_code_fsm)
    await state.set_state(VerificationState.waiting_answer)
    await state.update_data(answer=answer, referral_code=ref_code_fsm)

    await callback.message.answer(
        "🔐 <b>Verification Required</b>\n\n"
        f"Solve this to continue:\n\n"
        f"<code>{question}</code>\n\n"
        "Reply with the number only.",
        parse_mode="HTML"
    )


@router.callback_query(F.data == "resume_opted_in")
async def resume_opted_in(callback: CallbackQuery, state: FSMContext, pool: asyncpg.Pool):
    if callback.from_user.id != callback.message.chat.id:
        await callback.answer()
        return

    await set_user_opted_out(pool, callback.from_user.id, False)
    await callback.answer("✅ Resumed!")

    user = await get_user(pool, callback.from_user.id)
    name = user['anonymous_name'] if user else "User"

    await callback.message.edit_text(
        f"✅ You're active as <b>{name}</b>.\n"
        f"You will receive media from other users again.\n"
        f"Use /me to see your stats.",
        parse_mode="HTML"
    )
    await state.clear()


@router.callback_query(F.data == "stay_opted_out")
async def stay_opted_out(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != callback.message.chat.id:
        await callback.answer()
        return

    await callback.answer()
    await callback.message.edit_text(
        "You remain opted out. Use /start anytime to resume.",
        parse_mode="HTML"
    )
    await state.clear()


@router.message(F.text.startswith('/'), OnboardingState.disclaimer, OnboardingState.terms)
async def onboarding_command_override(message: Message, state: FSMContext, pool: asyncpg.Pool):
    """Allow commands like /admin to work even during onboarding states."""
    await state.clear()
    # Re-process the command now that state is cleared
    cmd = message.text.split()[0].lower()
    if cmd == "/start":
        await cmd_start(message, state, pool)
    elif cmd == "/admin":
        from handlers.admin import cmd_admin
        await cmd_admin(message, state)


@router.message(VerificationState.waiting_answer, ~F.text.startswith('/'))
async def process_verification(message: Message, state: FSMContext, pool: asyncpg.Pool):
    # Read answer from DB (not FSM) so verification survives bot restarts
    pending = await get_pending_verification(pool, message.from_user.id)
    if not pending:
        # Verification expired or bot restarted — restart the flow
        await state.clear()
        await message.answer("⚠️ Verification expired. Please /start again.")
        return

    correct = pending['answer']

    try:
        user_answer = int(message.text.strip())
    except (ValueError, AttributeError):
        question, new_answer = make_math_question()
        reserved_name = pending['reserved_name']
        # referral_code preserved in DB via COALESCE — no need to re-pass
        await save_pending_verification(pool, message.from_user.id, new_answer, reserved_name)
        existing_data = await state.get_data()
        await state.update_data(answer=new_answer, referral_code=existing_data.get('referral_code'))
        await message.answer(f"❌ Numbers only. Try again:\n\n<code>{question}</code>")
        return

    if user_answer != correct:
        question, new_answer = make_math_question()
        reserved_name = pending['reserved_name']
        # referral_code preserved in DB via COALESCE — no need to re-pass
        await save_pending_verification(pool, message.from_user.id, new_answer, reserved_name)
        existing_data = await state.get_data()
        await state.update_data(answer=new_answer, referral_code=existing_data.get('referral_code'))
        await message.answer(f"❌ Wrong. Try this one:\n\n<code>{question}</code>")
        return

    # Read referral_code from DB (primary) and FSM (fallback) BEFORE clearing state
    data = await state.get_data()
    referral_code_from_link = pending.get('referral_code') or data.get('referral_code')
    await state.clear()

    # Optimized verification context fetch
    context = await get_verification_context(pool, message.from_user.id)
    if not context['success']:
        await message.answer("⚠️ Database is busy. Please try again in a moment.")
        return

    pending = context['pending']
    config = context['config']

    if pending:
        name = pending['reserved_name']
    else:
        name = generate_anonymous_name()
        while await name_exists(pool, name):
            name = generate_anonymous_name()

    threshold = int(config.get('activation_threshold', '10'))

    # --- Activation Animation (2 frames instead of 5 — fewer API calls to same user) ---
    from utils.limiter import global_rate_limiter
    await global_rate_limiter.consume_for_user(message.from_user.id, priority=True)
    anim1 = await message.answer("🔄 <b>Activating your account</b> ⠋", parse_mode="HTML")
    await asyncio.sleep(0.6)
    await anim1.edit_text("🔄 <b>Activating your account</b> ⠸", parse_mode="HTML")
    await asyncio.sleep(0.6)

    # Create user
    await create_user(pool, message.from_user.id, name)
    await clear_pending_verification(pool, message.from_user.id)

    # Generate and assign referral code
    ref_code = _generate_referral_code()
    for _ in range(10):
        existing = await get_user_by_referral_code(pool, ref_code)
        if not existing:
            break
        ref_code = _generate_referral_code()
    await set_referral_code(pool, message.from_user.id, ref_code)

    # Handle referral tracking (store referred_by only — EXP awarded at activation)
    if referral_code_from_link:
        logger.info(f"Processing referral: user={message.from_user.id}, code={referral_code_from_link}")
        referrer = await get_user_by_referral_code(pool, referral_code_from_link)
        if referrer and referrer['id'] != message.from_user.id:
            logger.info(f"Valid referral: assigning user {message.from_user.id} to referrer {referrer['id']}")
            await set_referred_by(pool, message.from_user.id, referrer['id'])
            # Notify referrer that someone signed up via their link
            try:
                from utils.limiter import global_rate_limiter
                await global_rate_limiter.consume_for_user(referrer['id'], priority=True)
                await message.bot.send_message(
                    referrer['id'],
                    "👋 <b>New referral signed up!</b>\n\n"
                    "They need to activate their account first.\n"
                    "You'll receive your EXP bonus when they activate!",
                    parse_mode="HTML"
                )
            except Exception:
                pass
        else:
            logger.warning(f"Invalid referral code: {referral_code_from_link} (referrer not found or self-referral)")
    else:
        logger.info(f"User {message.from_user.id} joined without referral code")

    # Final animation frame
    await global_rate_limiter.consume_for_user(message.from_user.id, priority=True)
    await anim1.edit_text("✅ <b>Account Activated!</b>", parse_mode="HTML")
    await asyncio.sleep(0.6)
    await global_rate_limiter.consume_for_user(message.from_user.id, priority=True)
    await anim1.delete()

    # Welcome message
    await global_rate_limiter.consume_for_user(message.from_user.id, priority=True)
    await message.answer(
        f"🎉 <b>Welcome aboard, {name}!</b>\n\n"
        f"Your anonymous identity: <b>{name}</b>\n"
        f"This name is permanent and cannot be changed.\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📤 <b>To activate your account:</b>\n"
        f"Upload <b>{threshold} media files</b> (photos, videos, or documents).\n\n"
        f"⚠️ Captions with links will be rejected.\n"
        f"Once active, you will receive media from other users.\n\n"
        f"Use /help to see all commands.",
        parse_mode="HTML"
    )

    # Send referral card
    invite_key = await get_invite_key(pool)
    link = f"https://t.me/{BOT_USERNAME}?start={invite_key}--{ref_code}"
    await global_rate_limiter.consume_for_user(message.from_user.id, priority=True)
    await message.answer(
        "🎁 <b>Invite Friends & Earn</b>\n\n"
        "🔗 <b>Your Personal Invite Link</b>\n\n"
        f"<code>{link}</code>\n\n"
        "📊 <b>0</b> users joined via your link\n\n"
        "Share it with friends to grow the community! 🚀",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📋 Copy Link", callback_data="copy_referral_link")]
        ])
    )


@router.callback_query(F.data == "copy_referral_link")
async def copy_referral_link(callback: CallbackQuery):
    await callback.answer("📋 Link copied to clipboard!")
