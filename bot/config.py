import os

BOT_TOKEN = os.environ["BOT_TOKEN"]

_raw_admin = os.environ["ADMIN_ID"]
ADMIN_IDS = set(int(i.strip()) for i in _raw_admin.split(",") if i.strip())
ADMIN_ID = next(iter(ADMIN_IDS))  # Primary admin ID (backwards compat)

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

DATABASE_URL = os.environ["DATABASE_URL"]
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
