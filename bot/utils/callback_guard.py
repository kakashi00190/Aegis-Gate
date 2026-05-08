"""Lightweight callback spam protection.
Prevents excessive repeated callback clicks under 0.7 seconds.
Does NOT slow normal usage — only blocks rapid-fire duplicates."""
import time
import logging

logger = logging.getLogger(__name__)

# Tracks last callback time per (user_id, callback_data)
_callback_timestamps: dict[tuple[int, str], float] = {}
_COOLDOWN = 0.7  # seconds
_CLEANUP_INTERVAL = 500  # clean every N checks
_check_count = 0


def is_callback_spam(user_id: int, callback_data: str) -> bool:
    """Return True if this callback is a rapid-fire duplicate (< 0.7s)."""
    global _check_count
    _check_count += 1

    now = time.monotonic()
    key = (user_id, callback_data)
    last = _callback_timestamps.get(key)

    if last and now - last < _COOLDOWN:
        return True  # spam

    _callback_timestamps[key] = now

    # Periodic cleanup to prevent unbounded growth
    if _check_count % _CLEANUP_INTERVAL == 0:
        expired = [k for k, v in _callback_timestamps.items() if now - v > 60]
        for k in expired:
            del _callback_timestamps[k]

    return False
