import re
from datetime import datetime, timezone

LINK_PATTERN = re.compile(r'(https?://|t\.me/|@\w+)', re.IGNORECASE)


def contains_link(text: str) -> bool:
    if not text:
        return False
    return bool(LINK_PATTERN.search(text))


def format_timedelta_until(dt: datetime) -> str:
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = dt - now
    if delta.total_seconds() <= 0:
        return "now"
    total = int(delta.total_seconds())
    h = total // 3600
    m = (total % 3600) // 60
    if h > 0:
        return f"{h}h {m}m"
    return f"{m}m"


def format_datetime(dt: datetime) -> str:
    if dt is None:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def badge_for_rank(rank: int) -> str:
    if rank == 1:
        return "👑"
    elif rank == 2:
        return "🥈"
    elif rank == 3:
        return "🥉"
    elif rank <= 5:
        return "🎖️"
    elif rank <= 10:
        return "⭐"
    return ""


def medal_for_rank(rank: int) -> str:
    medals = {1: "👑", 2: "🥈", 3: "🥉"}
    return medals.get(rank, f"#{rank}")


def get_badge_display(badges: str, max_shown: int = 3) -> str:
    if not badges:
        return "—"
    badge_list = [b.strip() for b in badges.split(",") if b.strip()]
    if not badge_list:
        return "—"
    if len(badge_list) <= max_shown:
        return " ".join(badge_list)
    shown = " ".join(badge_list[:max_shown])
    extra = len(badge_list) - max_shown
    return f"{shown} +{extra}"


def get_all_badges(badges: str) -> str:
    if not badges:
        return "—"
    badge_list = [b.strip() for b in badges.split(",") if b.strip()]
    return " ".join(badge_list) if badge_list else "—"
