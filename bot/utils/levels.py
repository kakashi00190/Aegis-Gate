def required_exp(level: int) -> int:
    return 100 * (level ** 2)


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
