def required_exp(level: int) -> int:
    """Exponential XP curve: each level requires significantly more than the last.
    Level 1→2: 200 XP, 2→3: 600 XP, 3→4: 1400 XP, 5→6: 3800 XP,
    10→11: 22,000 XP, 20→21: 168,000 XP, 50→51: 2,550,000 XP
    Formula: 200 * level * (1.4 ^ level) — rounded to clean numbers."""
    base = 200 * level * (1.4 ** level)
    # Round to nearest 100 for clean display
    return int(round(base / 100) * 100)


def calculate_level(exp: int) -> int:
    level = 1
    while exp >= required_exp(level + 1):
        level += 1
    return level


def exp_progress(exp: int, level: int) -> tuple[int, int]:
    prev = required_exp(level) if level > 1 else 0
    next_ = required_exp(level + 1)
    current = max(0, exp - prev)
    return current, next_ - prev


def format_level_bar(exp: int, level: int) -> str:
    current, total = exp_progress(exp, level)
    bar_length = 12
    filled = int((current / total) * bar_length) if total > 0 else 0
    bar = "█" * filled + "░" * (bar_length - filled)
    return f"[{bar}] {current}/{total}"


# Referral badge tiers: (threshold, emoji, title)
REFERRAL_BADGES = [
    (5,    "🥉", "Recruiter"),
    (15,   "🥈", "Squad Leader"),
    (30,   "🥇", "Commander"),
    (50,   "💎", "Diamond Recruiter"),
    (100,  "🌟", "Legend Recruiter"),
    (200,  "🔱", "Mythic Recruiter"),
    (500,  "👁", "Supreme Recruiter"),
]


def referral_badge_for_count(count: int) -> tuple[str, str] | None:
    """Returns (emoji, title) for the highest referral badge earned, or None."""
    result = None
    for threshold, emoji, title in REFERRAL_BADGES:
        if count >= threshold:
            result = (emoji, title)
    return result


def referral_exp_bonus(referrer_level: int) -> int:
    """EXP bonus per activated referral. Scales with referrer's level."""
    return 100 + (referrer_level * 25)
