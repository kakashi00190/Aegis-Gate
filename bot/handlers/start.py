import random
import logging
import time
from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

import asyncpg
from database import (
    get_user, create_user, name_exists, get_config, mark_user_unblocked,
    save_pending_verification, get_pending_verification, clear_pending_verification,
    is_session_paused
)
from utils.names import generate_anonymous_name
from utils.helpers import format_timedelta_until

logger = logging.getLogger(__name__)
router = Router()

_start_cooldowns: dict[int, float] = {}
START_COOLDOWN = 5
COOLDOWN_TTL = 600


def _cleanup_cooldowns():
    now = time.time()
    expired = [k for k, v in _start_cooldowns.items() if now - v > COOLDOWN_TTL]
    for k in expired:
        del _start_cooldowns[k]


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
    user_id = message.from_user.id

    now = time.time()
    _cleanup_cooldowns()
    if now - _start_cooldowns.get(user_id, 0) < START_COOLDOWN:
        return
    _start_cooldowns[user_id] = now

    user = await get_user(pool, user_id)

    if user:
        if user['status'] == 'banned':
            await message.answer("🚫 You are permanently banned from this bot.")
            return

        await mark_user_unblocked(pool, user_id)

        paused, pause_until = await is_session_paused(pool)
        pause_note = ""
        if paused:
            time_left = format_timedelta_until(pause_until)
            pause_note = (
                f"\n\n⏸ <b>Session is transitioning.</b>\n"
                f"Uploads resume in <b>{time_left}</b>."
            )

        config = await get_config(pool)
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

    pending = await get_pending_verification(pool, user_id)
    if pending:
        reserved_name = pending['reserved_name']
        question, new_answer = make_math_question()
        await save_pending_verification(pool, user_id, new_answer, reserved_name)
        await state.set_state(VerificationState.waiting_answer)
        await state.update_data(answer=new_answer)
        await message.answer(
            "🔐 <b>Verification Required</b>\n\n"
            f"Solve this to continue:\n\n"
            f"<code>{question}</code>\n\n"
            "Reply with the number only."
        )
        return

    name = generate_anonymous_name()
    attempts = 0
    while await name_exists(pool, name) and attempts < 20:
        name = generate_anonymous_name()
        attempts += 1

    question, answer = make_math_question()
    await save_pending_verification(pool, user_id, answer, name)
    await state.set_state(VerificationState.waiting_answer)
    await state.update_data(answer=answer)

    await message.answer(
        "🔐 <b>Verification Required</b>\n\n"
        f"Solve this to continue:\n\n"
        f"<code>{question}</code>\n\n"
        "Reply with the number only."
    )


@router.message(VerificationState.waiting_answer)
async def process_verification(message: Message, state: FSMContext, pool: asyncpg.Pool):
    data = await state.get_data()
    correct = data.get('answer')

    try:
        user_answer = int(message.text.strip())
    except (ValueError, AttributeError):
        question, new_answer = make_math_question()
        pending = await get_pending_verification(pool, message.from_user.id)
        reserved_name = pending['reserved_name'] if pending else generate_anonymous_name()
        await save_pending_verification(pool, message.from_user.id, new_answer, reserved_name)
        await state.update_data(answer=new_answer)
        await message.answer(f"❌ Numbers only. Try again:\n\n<code>{question}</code>")
        return

    if user_answer != correct:
        question, new_answer = make_math_question()
        pending = await get_pending_verification(pool, message.from_user.id)
        reserved_name = pending['reserved_name'] if pending else generate_anonymous_name()
        await save_pending_verification(pool, message.from_user.id, new_answer, reserved_name)
        await state.update_data(answer=new_answer)
        await message.answer(f"❌ Wrong. Try this one:\n\n<code>{question}</code>")
        return

    await state.clear()

    pending = await get_pending_verification(pool, message.from_user.id)
    if pending:
        name = pending['reserved_name']
    else:
        name = generate_anonymous_name()
        while await name_exists(pool, name):
            name = generate_anonymous_name()

    config = await get_config(pool)
    threshold = int(config.get('activation_threshold', '10'))

    await create_user(pool, message.from_user.id, name)
    await clear_pending_verification(pool, message.from_user.id)

    await message.answer(
        f"✅ <b>Verified!</b>\n\n"
        f"Your anonymous identity: <b>{name}</b>\n"
        f"This name is permanent and cannot be changed.\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📤 <b>To activate your account:</b>\n"
        f"Upload <b>{threshold} media files</b> (photos, videos, or documents).\n\n"
        f"⚠️ Captions with links will be rejected.\n"
        f"Once active, you will receive media from other users.\n\n"
        f"Use /help to see all commands."
    )
