import asyncpg
import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Callable, Coroutine

logger = logging.getLogger(__name__)

from utils.levels import calculate_level
from utils.helpers import badge_for_rank

INIT_SQL = """
CREATE TABLE IF NOT EXISTS users (
    id BIGINT PRIMARY KEY,
    anonymous_name TEXT UNIQUE NOT NULL,
    status TEXT DEFAULT 'pending',
    exp INTEGER DEFAULT 0,
    level INTEGER DEFAULT 1,
    total_media_lifetime INTEGER DEFAULT 0,
    session_upload_count INTEGER DEFAULT 0,
    uploads_since_inactive INTEGER DEFAULT 0,
    badge_emoji TEXT DEFAULT '',
    bot_blocked BOOLEAN DEFAULT FALSE,
    last_activity_at TIMESTAMP WITH TIME ZONE,
    joined_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

ALTER TABLE users ADD COLUMN IF NOT EXISTS bot_blocked BOOLEAN DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS sessions (
    id SERIAL PRIMARY KEY,
    session_number INTEGER UNIQUE NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    ended_at TIMESTAMP WITH TIME ZONE,
    pause_until TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS media (
    id SERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES users(id),
    session_id INTEGER REFERENCES sessions(id),
    file_id TEXT NOT NULL,
    file_unique_id TEXT,
    media_type TEXT NOT NULL,
    media_group_id TEXT,
    scheduled_at TIMESTAMP WITH TIME ZONE,
    sent_at TIMESTAMP WITH TIME ZONE,
    claimed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

ALTER TABLE media ADD COLUMN IF NOT EXISTS media_group_id TEXT;
ALTER TABLE media ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMP WITH TIME ZONE;

DROP INDEX IF EXISTS idx_media_queue;
CREATE INDEX IF NOT EXISTS idx_media_queue ON media(scheduled_at)
    WHERE sent_at IS NULL AND scheduled_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_users_status ON users(status);
CREATE INDEX IF NOT EXISTS idx_users_activity ON users(last_activity_at)
    WHERE status = 'active';

CREATE TABLE IF NOT EXISTS reports (
    id SERIAL PRIMARY KEY,
    reporter_id BIGINT REFERENCES users(id),
    media_id INTEGER,
    uploader_id BIGINT,
    uploader_name TEXT,
    media_file_id TEXT,
    media_type TEXT,
    reported_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    solved BOOLEAN DEFAULT FALSE,
    solved_at TIMESTAMP WITH TIME ZONE,
    admin_message_id INTEGER
);

CREATE TABLE IF NOT EXISTS sent_messages (
    id SERIAL PRIMARY KEY,
    recipient_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    session_id INTEGER NOT NULL,
    media_id INTEGER REFERENCES media(id) ON DELETE SET NULL,
    sent_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

ALTER TABLE sent_messages DROP CONSTRAINT IF EXISTS sent_messages_media_id_fkey;
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'sent_messages_media_id_fkey') THEN
        ALTER TABLE sent_messages ADD CONSTRAINT sent_messages_media_id_fkey FOREIGN KEY (media_id) REFERENCES media(id) ON DELETE SET NULL;
    END IF;
END $$;

ALTER TABLE sent_messages ADD COLUMN IF NOT EXISTS media_id INTEGER REFERENCES media(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_sent_messages_session ON sent_messages(session_id);
CREATE INDEX IF NOT EXISTS idx_sent_messages_recipient ON sent_messages(recipient_id);

CREATE TABLE IF NOT EXISTS pending_verifications (
    user_id BIGINT PRIMARY KEY,
    answer INTEGER NOT NULL,
    reserved_name TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS admin_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT INTO admin_config (key, value) VALUES
    ('broadcast_delay_seconds', '30'),
    ('inactivity_minutes', '160'),
    ('session_duration_days', '7'),
    ('leaderboard_top', '10'),
    ('session_pause_hours', '3'),
    ('activation_threshold', '10'),
    ('reactivation_threshold', '3')
ON CONFLICT (key) DO NOTHING;

INSERT INTO sessions (session_number)
SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM sessions);
"""


async def init_db(pool: asyncpg.Pool):
    """Initializes the database schema using fast checks and individual statement execution.
    Uses near-instant regclass checks instead of information_schema."""
    
    table_checks = [
        ('users', """CREATE TABLE IF NOT EXISTS users (
            id BIGINT PRIMARY KEY,
            anonymous_name TEXT UNIQUE NOT NULL,
            status TEXT DEFAULT 'pending',
            exp INTEGER DEFAULT 0,
            level INTEGER DEFAULT 1,
            total_media_lifetime INTEGER DEFAULT 0,
            session_upload_count INTEGER DEFAULT 0,
            uploads_since_inactive INTEGER DEFAULT 0,
            badge_emoji TEXT DEFAULT '',
            bot_blocked BOOLEAN DEFAULT FALSE,
            last_activity_at TIMESTAMP WITH TIME ZONE,
            joined_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )"""),
        ('sessions', """CREATE TABLE IF NOT EXISTS sessions (
            id SERIAL PRIMARY KEY,
            session_number INTEGER UNIQUE NOT NULL,
            started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            ended_at TIMESTAMP WITH TIME ZONE,
            pause_until TIMESTAMP WITH TIME ZONE
        )"""),
        ('media', """CREATE TABLE IF NOT EXISTS media (
            id SERIAL PRIMARY KEY,
            user_id BIGINT REFERENCES users(id),
            session_id INTEGER REFERENCES sessions(id),
            file_id TEXT NOT NULL,
            file_unique_id TEXT,
            media_type TEXT NOT NULL,
            media_group_id TEXT,
            scheduled_at TIMESTAMP WITH TIME ZONE,
            sent_at TIMESTAMP WITH TIME ZONE,
            claimed_at TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )"""),
        ('reports', """CREATE TABLE IF NOT EXISTS reports (
            id SERIAL PRIMARY KEY,
            reporter_id BIGINT REFERENCES users(id),
            media_id INTEGER,
            uploader_id BIGINT,
            uploader_name TEXT,
            media_file_id TEXT,
            media_type TEXT,
            reported_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            solved BOOLEAN DEFAULT FALSE,
            solved_at TIMESTAMP WITH TIME ZONE,
            admin_message_id INTEGER
        )"""),
        ('sent_messages', """CREATE UNLOGGED TABLE IF NOT EXISTS sent_messages (
            id SERIAL PRIMARY KEY,
            recipient_id BIGINT NOT NULL,
            message_id BIGINT NOT NULL,
            session_id INTEGER NOT NULL,
            media_id INTEGER REFERENCES media(id) ON DELETE SET NULL,
            sent_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )"""),
        ('pending_verifications', """CREATE TABLE IF NOT EXISTS pending_verifications (
            user_id BIGINT PRIMARY KEY,
            answer INTEGER NOT NULL,
            reserved_name TEXT NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )"""),
        ('admin_config', """CREATE TABLE IF NOT EXISTS admin_config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )""")
    ]

    migrations = [
        # (check_sql, migration_sql, label)
        (
            "SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='bot_blocked'",
            "ALTER TABLE users ADD COLUMN bot_blocked BOOLEAN DEFAULT FALSE",
            "users_bot_blocked"
        ),
        (
            "SELECT 1 FROM information_schema.columns WHERE table_name='media' AND column_name='media_group_id'",
            "ALTER TABLE media ADD COLUMN media_group_id TEXT",
            "media_group_id"
        ),
        (
            "SELECT 1 FROM information_schema.columns WHERE table_name='media' AND column_name='claimed_at'",
            "ALTER TABLE media ADD COLUMN claimed_at TIMESTAMP WITH TIME ZONE",
            "media_claimed_at"
        ),
        (
            "SELECT 1 FROM information_schema.columns WHERE table_name='sent_messages' AND column_name='media_id'",
            "ALTER TABLE sent_messages ADD COLUMN media_id INTEGER REFERENCES media(id) ON DELETE SET NULL",
            "sent_messages_media_id"
        ),
        (
            "SELECT 1 FROM pg_indexes WHERE indexname='idx_media_queue'",
            "CREATE INDEX idx_media_queue ON media(scheduled_at) WHERE sent_at IS NULL AND scheduled_at IS NOT NULL",
            "idx_media_queue"
        ),
        (
            "SELECT 1 FROM pg_indexes WHERE indexname='idx_users_status'",
            "CREATE INDEX idx_users_status ON users(status)",
            "idx_users_status"
        ),
        (
            "SELECT 1 FROM pg_indexes WHERE indexname='idx_users_activity'",
            "CREATE INDEX idx_users_activity ON users(last_activity_at) WHERE status = 'active'",
            "idx_users_activity"
        ),
        (
            "SELECT 1 FROM pg_indexes WHERE indexname='idx_users_lower_name'",
            "CREATE INDEX idx_users_lower_name ON users(LOWER(anonymous_name))",
            "idx_users_lower_name"
        ),
        (
            "SELECT 1 FROM pg_indexes WHERE indexname='idx_sent_messages_session'",
            "CREATE INDEX idx_sent_messages_session ON sent_messages(session_id)",
            "idx_sent_messages_session"
        ),
        (
            "SELECT 1 FROM pg_indexes WHERE indexname='idx_sent_messages_recipient'",
            "CREATE INDEX idx_sent_messages_recipient ON sent_messages(recipient_id)",
            "idx_sent_messages_recipient"
        ),
        (
            "SELECT 1 FROM pg_indexes WHERE indexname='idx_users_session_uploads'",
            "CREATE INDEX idx_users_session_uploads ON users(session_upload_count DESC)",
            "idx_users_session_uploads"
        ),
        (
            "SELECT 1 FROM pg_indexes WHERE indexname='idx_users_lifetime_media'",
            "CREATE INDEX idx_users_lifetime_media ON users(total_media_lifetime DESC)",
            "idx_users_lifetime_media"
        )
    ]

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Database initialization attempt {attempt}/{max_retries}...")
            async with pool.acquire() as conn:
                # Use a very long timeout for the whole initialization process
                # Supabase Nano can be extremely slow under load (Unhealthy state)
                async with asyncio.timeout(900): # 15 minutes for the whole init
                    # 1. Check and Create Tables (Using regclass for speed)
                    for table_name, create_stmt in table_checks:
                        try:
                            exists = await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", table_name)
                            if not exists:
                                logger.info(f"  Creating table: {table_name}")
                                await conn.execute(create_stmt)
                        except Exception as e:
                            logger.warning(f"  ⚠️ Table check/create failed for {table_name}: {repr(e)}")

                    # 2. Run Migrations (Patiently)
                    # We check if the column/index exists FIRST to avoid long locks or timeouts
                    # even if it's already there.
                    for check_sql, stmt, label in migrations:
                        try:
                            exists = await conn.fetchval(check_sql)
                            if not exists:
                                logger.info(f"  Running migration: {label}")
                                # Use separate timeout for each migration statement
                                async with asyncio.timeout(300):
                                    await conn.execute(stmt)
                        except Exception as e:
                            # Still handle "already exists" just in case of race conditions
                            if "already exists" not in str(e).lower() and "duplicate column" not in str(e).lower():
                                logger.warning(f"  ⚠️ Migration failed ({label}): {repr(e)}")

                    # 3. Seed Default Data
                    try:
                        await conn.execute("""
                            INSERT INTO admin_config (key, value) VALUES
                                ('broadcast_delay_seconds', '30'),
                                ('inactivity_minutes', '160'),
                                ('session_duration_days', '7'),
                                ('leaderboard_top', '10'),
                                ('session_pause_hours', '3'),
                                ('activation_threshold', '10'),
                                ('reactivation_threshold', '3')
                            ON CONFLICT (key) DO NOTHING
                        """)
                        await conn.execute(
                            "INSERT INTO sessions (session_number) SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM sessions)"
                        )
                    except Exception as e:
                        logger.warning(f"  ⚠️ Seeding data failed: {repr(e)}")

            logger.info("✅ Database schema initialization complete.")
            return
        except Exception as e:
            logger.error(f"❌ Initialization failed on attempt {attempt}: {repr(e)}")
            if attempt == max_retries:
                raise
            await asyncio.sleep(5)


async def get_config(pool: asyncpg.Pool) -> Dict[str, str]:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                rows = await conn.fetch("SELECT key, value FROM admin_config")
                return {row['key']: row['value'] for row in rows}
    except Exception as e:
        logger.error(f"Error fetching config: {repr(e)}")
        return {}


async def set_config(pool: asyncpg.Pool, key: str, value: str):
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO admin_config (key, value) VALUES ($1, $2) "
                    "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                    key, value
                )
    except Exception as e:
        logger.error(f"Error setting config {key}: {repr(e)}")


async def get_user(pool: asyncpg.Pool, user_id: int) -> Optional[asyncpg.Record]:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                return await conn.fetchrow("SELECT * FROM users WHERE id = $1", user_id)
    except Exception as e:
        logger.error(f"Error fetching user {user_id}: {repr(e)}")
        return None


async def get_upload_context(pool: asyncpg.Pool, user_id: int) -> dict:
    """Fetches user, current session, and config with retries."""
    for attempt in range(1, 4):
        try:
            async with asyncio.timeout(15):
                async with pool.acquire() as conn:
                    user = await conn.fetchrow("SELECT * FROM users WHERE id = $1", user_id)
                    session = await conn.fetchrow("SELECT * FROM sessions ORDER BY id DESC LIMIT 1")
                    config_rows = await conn.fetch("SELECT key, value FROM admin_config")
                    
                    config = {row['key']: row['value'] for row in config_rows}
                    return {
                        'user': user,
                        'session': session,
                        'config': config,
                        'success': True
                    }
        except Exception as e:
            if attempt < 3:
                logger.warning(f"Retry {attempt} for upload context {user_id}: {repr(e)}")
                await asyncio.sleep(1)
                continue
            logger.error(f"Final error fetching upload context for {user_id}: {repr(e)}")
            return {'success': False, 'error': str(e)}
    return {'success': False, 'error': 'Max retries reached'}


async def get_start_context(pool: asyncpg.Pool, user_id: int) -> dict:
    """Fetches user, pending verification, current session, and config with retries."""
    for attempt in range(1, 4):
        try:
            async with asyncio.timeout(15):
                async with pool.acquire() as conn:
                    user = await conn.fetchrow("SELECT * FROM users WHERE id = $1", user_id)
                    pending = await conn.fetchrow("SELECT * FROM pending_verifications WHERE user_id = $1", user_id)
                    session = await conn.fetchrow("SELECT * FROM sessions ORDER BY id DESC LIMIT 1")
                    config_rows = await conn.fetch("SELECT key, value FROM admin_config")
                    
                    config = {row['key']: row['value'] for row in config_rows}
                    return {
                        'user': user,
                        'pending': pending,
                        'session': session,
                        'config': config,
                        'success': True
                    }
        except Exception as e:
            if attempt < 3:
                logger.warning(f"Retry {attempt} for start context {user_id}: {repr(e)}")
                await asyncio.sleep(1)
                continue
            logger.error(f"Final error fetching start context for {user_id}: {repr(e)}")
            return {'success': False, 'error': str(e)}
    return {'success': False, 'error': 'Max retries reached'}


async def get_verification_context(pool: asyncpg.Pool, user_id: int) -> dict:
    """Fetches pending verification and config with retries."""
    for attempt in range(1, 4):
        try:
            async with asyncio.timeout(15):
                async with pool.acquire() as conn:
                    pending = await conn.fetchrow("SELECT * FROM pending_verifications WHERE user_id = $1", user_id)
                    config_rows = await conn.fetch("SELECT key, value FROM admin_config")
                    
                    config = {row['key']: row['value'] for row in config_rows}
                    return {
                        'pending': pending,
                        'config': config,
                        'success': True
                    }
        except Exception as e:
            if attempt < 3:
                logger.warning(f"Retry {attempt} for verification context {user_id}: {repr(e)}")
                await asyncio.sleep(1)
                continue
            logger.error(f"Final error fetching verification context for {user_id}: {repr(e)}")
            return {'success': False, 'error': str(e)}
    return {'success': False, 'error': 'Max retries reached'}


async def get_user_by_name(pool: asyncpg.Pool, name: str) -> Optional[asyncpg.Record]:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                return await conn.fetchrow(
                    "SELECT * FROM users WHERE LOWER(anonymous_name) = LOWER($1)", name
                )
    except Exception as e:
        logger.error(f"Error fetching user by name {name}: {repr(e)}")
        return None


async def get_user_by_id_or_name(pool: asyncpg.Pool, query: str) -> Optional[asyncpg.Record]:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                try:
                    user_id = int(query)
                    result = await conn.fetchrow("SELECT * FROM users WHERE id = $1", user_id)
                    if result:
                        return result
                except ValueError:
                    pass
                return await conn.fetchrow(
                    "SELECT * FROM users WHERE LOWER(anonymous_name) = LOWER($1)", query
                )
    except Exception as e:
        logger.error(f"Error fetching user by id or name {query}: {repr(e)}")
        return None


async def name_exists(pool: asyncpg.Pool, name: str) -> bool:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                return bool(await conn.fetchval(
                    "SELECT 1 FROM users WHERE LOWER(anonymous_name) = LOWER($1)", name
                ))
    except Exception as e:
        logger.error(f"Error checking name existence {name}: {repr(e)}")
        return False


async def save_pending_verification(pool: asyncpg.Pool, user_id: int, answer: int, reserved_name: str):
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO pending_verifications (user_id, answer, reserved_name) VALUES ($1, $2, $3) "
                    "ON CONFLICT (user_id) DO UPDATE SET answer = $2, reserved_name = $3, created_at = NOW()",
                    user_id, answer, reserved_name
                )
    except Exception as e:
        logger.error(f"Error saving verification for {user_id}: {repr(e)}")


async def get_pending_verification(pool: asyncpg.Pool, user_id: int) -> Optional[asyncpg.Record]:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                return await conn.fetchrow(
                    "SELECT * FROM pending_verifications WHERE user_id = $1", user_id
                )
    except Exception as e:
        logger.error(f"Error fetching verification for {user_id}: {repr(e)}")
        return None


async def clear_pending_verification(pool: asyncpg.Pool, user_id: int):
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM pending_verifications WHERE user_id = $1", user_id
                )
    except Exception as e:
        logger.error(f"Error clearing verification for {user_id}: {repr(e)}")


async def cleanup_stale_verifications(pool: asyncpg.Pool, max_age_hours: int = 24) -> int:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                result = await conn.execute(
                    "DELETE FROM pending_verifications WHERE created_at < NOW() - make_interval(hours => $1)",
                    max_age_hours
                )
                count = int(result.split()[-1]) if result else 0
                return count
    except Exception as e:
        logger.error(f"Error cleaning up verifications: {repr(e)}")
        return 0


async def create_user(pool: asyncpg.Pool, user_id: int, anonymous_name: str) -> asyncpg.Record:
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            async with asyncio.timeout(15): # Increased timeout
                async with pool.acquire() as conn:
                    return await conn.fetchrow(
                        "INSERT INTO users (id, anonymous_name, status) VALUES ($1, $2, 'pending') "
                        "ON CONFLICT (id) DO UPDATE SET anonymous_name = users.anonymous_name RETURNING *",
                        user_id, anonymous_name
                    )
        except asyncio.TimeoutError:
            if attempt == max_retries:
                logger.error(f"Final timeout error creating user {user_id}")
                raise
            logger.warning(f"Timeout creating user {user_id}, retrying ({attempt}/{max_retries})...")
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Error creating user {user_id}: {repr(e)}")
            raise


async def get_current_session(pool: asyncpg.Pool) -> Optional[asyncpg.Record]:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                return await conn.fetchrow(
                    "SELECT * FROM sessions ORDER BY id DESC LIMIT 1"
                )
    except Exception as e:
        logger.error(f"Error fetching current session: {repr(e)}")
        return None


async def is_session_paused(pool: asyncpg.Pool) -> tuple[bool, Optional[datetime]]:
    session = await get_current_session(pool)
    if session and session['pause_until']:
        now = datetime.now(timezone.utc)
        pause_until = session['pause_until']
        if pause_until.tzinfo is None:
            pause_until = pause_until.replace(tzinfo=timezone.utc)
        if now < pause_until:
            return True, pause_until
    return False, None


async def add_media(
    pool: asyncpg.Pool,
    user_id: int,
    session_id: int,
    file_id: str,
    file_unique_id: str,
    media_type: str,
    delay_seconds: int,
    media_group_id: Optional[str] = None
) -> Optional[asyncpg.Record]:
    """Adds media with staggered scheduling to handle high volume (100-1000+) gracefully.
    If multiple items are uploaded quickly, they are spread out in the queue."""
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                # Check if there's already media scheduled for this user in this session
                last_scheduled = await conn.fetchval(
                    "SELECT MAX(scheduled_at) FROM media WHERE user_id = $1 AND session_id = $2 AND sent_at IS NULL",
                    user_id, session_id
                )
                
                now = datetime.now(timezone.utc)
                base_time = now + timedelta(seconds=delay_seconds)
                
                if last_scheduled:
                    if last_scheduled.tzinfo is None:
                        last_scheduled = last_scheduled.replace(tzinfo=timezone.utc)
                    
                    # If the last item is scheduled far in the future, keep staggering
                    # We add 0.5 seconds between items from the same user to prevent flooding
                    scheduled_at = max(base_time, last_scheduled + timedelta(milliseconds=500))
                else:
                    scheduled_at = base_time

                return await conn.fetchrow(
                    """INSERT INTO media 
                       (user_id, session_id, file_id, file_unique_id, media_type, scheduled_at, media_group_id) 
                       VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *""",
                    user_id, session_id, file_id, file_unique_id, media_type, scheduled_at, media_group_id
                )
    except Exception as e:
        logger.error(f"Error adding media for {user_id}: {repr(e)}")
        return None


async def update_user_on_upload(
    pool: asyncpg.Pool,
    user_id: int,
    exp_gain: int = 10
) -> dict:
    """Updates user stats on upload using an atomic UPDATE to avoid row-level locking contention."""
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                # Use a single atomic UPDATE with RETURNING to get both old and new values
                # Level formula: level = floor(sqrt(exp / 100))
                # Note: This formula matches `required_exp(level) = 100 * (level ** 2)`
                updated = await conn.fetchrow(
                    """
                    WITH old_data AS (
                        SELECT level FROM users WHERE id = $1
                    )
                    UPDATE users SET
                        exp = exp + $2,
                        level = floor(sqrt((exp + $2) / 100.0)),
                        total_media_lifetime = total_media_lifetime + 1,
                        session_upload_count = session_upload_count + 1,
                        last_activity_at = NOW(),
                        bot_blocked = FALSE
                    FROM old_data
                    WHERE id = $1 
                    RETURNING users.*, old_data.level as old_level
                    """,
                    user_id, exp_gain
                )
                
                if not updated:
                    return {}

                new_level = int(updated['level'])
                old_level = int(updated['old_level'])

                return {
                    'user': updated,
                    'level_up': new_level > old_level,
                    'new_level': new_level,
                }
    except asyncio.TimeoutError:
        logger.error(f"Timeout updating user {user_id} on upload")
        return {}
    except Exception as e:
        logger.error(f"Error updating user {user_id} on upload: {repr(e)}")
        return {}


async def activate_user(pool: asyncpg.Pool, user_id: int) -> bool:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                result = await conn.fetchval(
                    "UPDATE users SET status = 'active', last_activity_at = NOW() "
                    "WHERE id = $1 AND status = 'pending' RETURNING id",
                    user_id
                )
                return bool(result)
    except Exception as e:
        logger.error(f"Error activating user {user_id}: {repr(e)}")
        return False


async def reactivate_user(pool: asyncpg.Pool, user_id: int) -> bool:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                result = await conn.fetchval(
                    "UPDATE users SET status = 'active', uploads_since_inactive = 0, "
                    "last_activity_at = NOW() WHERE id = $1 AND status = 'inactive' RETURNING id",
                    user_id
                )
                return bool(result)
    except Exception as e:
        logger.error(f"Error reactivating user {user_id}: {repr(e)}")
        return False


async def increment_inactive_uploads(pool: asyncpg.Pool, user_id: int) -> int:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                return await conn.fetchval(
                    "UPDATE users SET uploads_since_inactive = uploads_since_inactive + 1 "
                    "WHERE id = $1 RETURNING uploads_since_inactive",
                    user_id
                )
    except Exception as e:
        logger.error(f"Error incrementing inactive uploads for {user_id}: {repr(e)}")
        return 0


async def mark_user_blocked(pool: asyncpg.Pool, user_id: int):
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE users SET bot_blocked = TRUE WHERE id = $1",
                    user_id
                )
    except Exception as e:
        logger.error(f"Error marking user {user_id} as blocked: {repr(e)}")


async def mark_user_unblocked(pool: asyncpg.Pool, user_id: int):
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE users SET bot_blocked = FALSE WHERE id = $1",
                    user_id
                )
    except Exception as e:
        logger.error(f"Error marking user {user_id} as unblocked: {repr(e)}")


async def reset_all_blocked_status(pool: asyncpg.Pool) -> int:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                result = await conn.execute("UPDATE users SET bot_blocked = FALSE WHERE status != 'banned'")
                return int(result.split()[-1]) if result else 0
    except Exception as e:
        logger.error(f"Error resetting blocked status: {repr(e)}")
        return 0


async def get_user_rank(pool: asyncpg.Pool, user_id: int) -> int:
    try:
        async with asyncio.timeout(5):
            async with pool.acquire() as conn:
                # Optimized rank calculation: count users with more uploads
                rank = await conn.fetchval(
                    """
                    SELECT COUNT(*) + 1 
                    FROM users 
                    WHERE session_upload_count > (
                        SELECT session_upload_count FROM users WHERE id = $1
                    ) AND status != 'banned'
                    """,
                    user_id
                )
                return rank or 1
    except Exception as e:
        logger.debug(f"Error fetching rank for {user_id}: {repr(e)}")
        return 1


async def get_leaderboard(pool: asyncpg.Pool, limit: int = 10) -> List[asyncpg.Record]:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                return await conn.fetch(
                    """SELECT *, RANK() OVER (ORDER BY session_upload_count DESC) as rank
                       FROM users WHERE status != 'banned'
                       ORDER BY session_upload_count DESC LIMIT $1""",
                    limit
                )
    except Exception as e:
        logger.error(f"Error fetching leaderboard: {repr(e)}")
        return []


async def get_all_active_users(pool: asyncpg.Pool) -> List[asyncpg.Record]:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                return await conn.fetch(
                    "SELECT id FROM users WHERE status = 'active' AND bot_blocked = FALSE"
                )
    except Exception as e:
        logger.error(f"Error fetching active users: {repr(e)}")
        return []


async def count_active_users(pool: asyncpg.Pool) -> int:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                return await conn.fetchval(
                    "SELECT COUNT(*) FROM users WHERE status = 'active' AND bot_blocked = FALSE"
                )
    except Exception as e:
        logger.error(f"Error counting active users: {repr(e)}")
        return 0


async def get_all_notifiable_users(pool: asyncpg.Pool) -> List[asyncpg.Record]:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                return await conn.fetch(
                    "SELECT id, status FROM users WHERE status NOT IN ('banned') AND bot_blocked = FALSE"
                )
    except Exception as e:
        logger.error(f"Error fetching notifiable users: {repr(e)}")
        return []


# Global cache for advanced stats to save Disk IO on Supabase Nano
_stats_cache = {
    'data': None,
    'timestamp': 0
}
STATS_CACHE_TTL = 300 # 5 minutes cache

async def get_advanced_stats(pool: asyncpg.Pool) -> dict:
    now = time.time()
    if _stats_cache['data'] and now - _stats_cache['timestamp'] < STATS_CACHE_TTL:
        return _stats_cache['data']

    try:
        async with asyncio.timeout(30): # Increased to 30s
            async with pool.acquire() as conn:
                # 1. Combined User Stats (One table scan instead of many)
                user_stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as total_users,
                        COUNT(*) FILTER (WHERE status = 'active' AND bot_blocked = FALSE) as active_count,
                        COUNT(*) FILTER (WHERE status = 'inactive' AND bot_blocked = FALSE) as inactive_count,
                        COUNT(*) FILTER (WHERE status = 'pending' AND bot_blocked = FALSE) as pending_count,
                        COUNT(*) FILTER (WHERE status = 'banned') as banned_count,
                        COUNT(*) FILTER (WHERE bot_blocked = TRUE) as blocked_count,
                        COUNT(*) FILTER (WHERE last_activity_at > NOW() - INTERVAL '24 hours' AND bot_blocked = FALSE) as active_24h,
                        COUNT(*) FILTER (WHERE joined_at > NOW() - INTERVAL '24 hours') as joined_today,
                        COUNT(*) FILTER (WHERE joined_at > NOW() - INTERVAL '7 days') as joined_7d,
                        COALESCE(SUM(exp), 0) as total_exp
                    FROM users
                """)

                # 2. Combined Media Stats
                media_stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) FILTER (WHERE sent_at IS NULL AND scheduled_at IS NOT NULL) as in_queue,
                        COUNT(*) FILTER (WHERE sent_at > NOW() - INTERVAL '24 hours') as sent_today
                    FROM media
                """)

                # 3. Quick standalone counts
                unverified = await conn.fetchval("SELECT COUNT(*) FROM pending_verifications") or 0
                unsolved_reports = await conn.fetchval("SELECT COUNT(*) FROM reports WHERE solved = FALSE") or 0

                # 4. Session Info
                session = await conn.fetchrow("SELECT * FROM sessions ORDER BY id DESC LIMIT 1")
                session_total = 0
                if session:
                    session_total = await conn.fetchval(
                        "SELECT COUNT(*) FROM media WHERE session_id = $1", session['id']
                    ) or 0

                # 5. Top Users
                top3 = await conn.fetch("""
                    SELECT anonymous_name, session_upload_count 
                    FROM users 
                    WHERE session_upload_count > 0 AND status != 'banned'
                    ORDER BY session_upload_count DESC LIMIT 3
                """)

                active_count = user_stats['active_count'] or 0

                res = {
                    'total': (user_stats['total_users'] or 0) + unverified,
                    'total_users': user_stats['total_users'] or 0,
                    'active': active_count,
                    'inactive': user_stats['inactive_count'] or 0,
                    'pending': user_stats['pending_count'] or 0,
                    'banned': user_stats['banned_count'] or 0,
                    'blocked_bot': user_stats['blocked_count'] or 0,
                    'unverified': unverified,
                    'active_24h': int(user_stats['active_24h'] or 0),
                    'joined_today': int(user_stats['joined_today'] or 0),
                    'joined_7d': int(user_stats['joined_7d'] or 0),
                    'total_exp': user_stats['total_exp'] or 0,
                    'in_queue': int(media_stats['in_queue'] or 0),
                    'sent_today': int(media_stats['sent_today'] or 0),
                    'session': dict(session) if session else {},
                    'session_total': int(session_total),
                    'top3': [dict(u) for u in top3],
                    'unsolved_reports': int(unsolved_reports),
                    'avg_uploads': round(session_total / active_count, 1) if active_count > 0 else 0,
                    'last_updated': datetime.now(timezone.utc).isoformat(),
                    'status': 'ok'
                }
                
                # Update cache
                _stats_cache['data'] = res
                _stats_cache['timestamp'] = time.time()
                return res
    except asyncio.TimeoutError:
        logger.error("Timeout fetching advanced stats from database")
        return {'status': 'timeout', 'error': 'Database timeout'}
    except Exception as e:
        logger.error(f"Error fetching advanced stats: {repr(e)}")
        return {'status': 'error', 'error': str(e)}


async def get_stats(pool: asyncpg.Pool) -> dict:
    return await get_advanced_stats(pool)


async def get_unsolved_reports(
    pool: asyncpg.Pool,
    limit: int = 5,
    offset: int = 0
) -> List[asyncpg.Record]:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                return await conn.fetch(
                    "SELECT * FROM reports WHERE solved = FALSE "
                    "ORDER BY reported_at DESC LIMIT $1 OFFSET $2",
                    limit, offset
                )
    except Exception as e:
        logger.error(f"Error fetching unsolved reports: {repr(e)}")
        return []


async def count_unsolved_reports(pool: asyncpg.Pool) -> int:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                return await conn.fetchval("SELECT COUNT(*) FROM reports WHERE solved = FALSE") or 0
    except Exception as e:
        logger.error(f"Error counting unsolved reports: {repr(e)}")
        return 0


async def solve_report(pool: asyncpg.Pool, report_id: int):
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE reports SET solved = TRUE, solved_at = NOW() WHERE id = $1",
                    report_id
                )
    except Exception as e:
        logger.error(f"Error solving report {report_id}: {repr(e)}")


async def ban_user(pool: asyncpg.Pool, user_id: int):
    try:
        async with asyncio.timeout(15):
            async with pool.acquire() as conn:
                async with conn.transaction():
                    # 1. Set status to banned
                    await conn.execute(
                        "UPDATE users SET status = 'banned' WHERE id = $1 AND status != 'banned'",
                        user_id
                    )
                    # 2. Automatically solve ALL pending reports for this uploader
                    await conn.execute(
                        "UPDATE reports SET solved = TRUE, solved_at = NOW() WHERE uploader_id = $1 AND solved = FALSE",
                        user_id
                    )
    except Exception as e:
        logger.error(f"Error banning user {user_id}: {repr(e)}")


async def unban_user(pool: asyncpg.Pool, user_id: int):
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE users SET status = 'inactive' WHERE id = $1 AND status = 'banned'",
                    user_id
                )
    except Exception as e:
        logger.error(f"Error unbanning user {user_id}: {repr(e)}")


async def claim_due_broadcasts(pool: asyncpg.Pool, limit: int = 50) -> List[asyncpg.Record]:
    """Atomically fetch and mark media items as claimed in one query.
    Handles media groups by fetching all items in a group if any are due."""
    try:
        async with asyncio.timeout(60): # Increased timeout to 60s for high load
            async with pool.acquire() as conn:
                # First, find IDs of media items that are due
                # Using SKIP LOCKED to avoid waiting for other workers
                due_ids = await conn.fetch(
                    """SELECT id, media_group_id FROM media
                       WHERE scheduled_at <= NOW()
                         AND sent_at IS NULL
                         AND (claimed_at IS NULL OR claimed_at < NOW() - INTERVAL '5 minutes')
                       ORDER BY scheduled_at ASC
                       LIMIT $1
                       FOR UPDATE SKIP LOCKED""",
                    limit
                )
                
                if not due_ids:
                    return []

                # Collect all IDs to update, including whole media groups
                ids_to_claim = {row['id'] for row in due_ids}
                group_ids = {row['media_group_id'] for row in due_ids if row['media_group_id']}
                
                if group_ids:
                    # Execute this in the same transaction to keep it safe
                    extra_ids = await conn.fetch(
                        "SELECT id FROM media WHERE media_group_id = ANY($1) AND sent_at IS NULL",
                        list(group_ids)
                    )
                    for row in extra_ids:
                        ids_to_claim.add(row['id'])

                # Update and return in one atomic operation
                return await conn.fetch(
                    """WITH claimed AS (
                           UPDATE media SET claimed_at = NOW()
                           WHERE id = ANY($1)
                           RETURNING *
                       )
                       SELECT claimed.*, users.anonymous_name
                       FROM claimed
                       JOIN users ON claimed.user_id = users.id
                       ORDER BY claimed.created_at ASC""",
                    list(ids_to_claim)
                )
    except asyncio.TimeoutError:
        logger.error("Timeout claiming broadcasts from database (DB too busy - 60s exceeded)")
        return []
    except Exception as e:
        logger.error(f"Error claiming broadcasts: {repr(e)}")
        return []


async def mark_media_sent(pool: asyncpg.Pool, media_ids: List[int]):
    """Marks media items as sent successfully."""
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE media SET sent_at = NOW(), claimed_at = NULL WHERE id = ANY($1)",
                    media_ids
                )
    except Exception as e:
        logger.error(f"Error marking media as sent {media_ids}: {repr(e)}")


async def unclaim_broadcast(pool: asyncpg.Pool, media_ids: List[int], delay_seconds: int = 30):
    """Marks media items as not claimed and schedules them for a later time."""
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                await conn.execute(
                    """UPDATE media SET 
                       claimed_at = NULL, 
                       scheduled_at = NOW() + make_interval(secs => $1)
                       WHERE id = ANY($2)""",
                    delay_seconds, media_ids
                )
    except Exception as e:
        logger.error(f"Error unclaiming media {media_ids}: {repr(e)}")


async def get_inactive_users(pool: asyncpg.Pool, cutoff: datetime) -> List[asyncpg.Record]:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                return await conn.fetch(
                    """UPDATE users SET status = 'inactive', uploads_since_inactive = 0
                       WHERE status = 'active'
                         AND (last_activity_at IS NULL OR last_activity_at < $1)
                       RETURNING id""",
                    cutoff
                )
    except Exception as e:
        logger.error(f"Error fetching inactive users: {repr(e)}")
        return []


async def create_report(
    pool: asyncpg.Pool,
    reporter_id: int,
    media_id: int,
    uploader_id: int,
    uploader_name: str,
    media_file_id: str,
    media_type: str,
) -> asyncpg.Record:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                return await conn.fetchrow(
                    """INSERT INTO reports
                       (reporter_id, media_id, uploader_id, uploader_name, media_file_id, media_type)
                       VALUES ($1, $2, $3, $4, $5, $6) RETURNING *""",
                    reporter_id, media_id, uploader_id, uploader_name, media_file_id, media_type
                )
    except Exception as e:
        logger.error(f"Error creating report: {repr(e)}")
        raise


async def set_report_admin_message(pool: asyncpg.Pool, report_id: int, msg_id: int):
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE reports SET admin_message_id = $2 WHERE id = $1",
                    report_id, msg_id
                )
    except Exception as e:
        logger.error(f"Error setting report admin message: {repr(e)}")


async def end_session(
    pool: asyncpg.Pool,
    session_id: int,
    pause_hours: float,
    leaderboard_top: int,
    progress_callback: Optional[Callable[[str, int], Coroutine]] = None
) -> Optional[dict]:
    """Ends a session, handles badges, and resets stats. Media cleanup should be handled separately."""
    
    # 1. Close session record first (short transaction)
    async with pool.acquire() as conn:
        if progress_callback:
            await progress_callback("Closing session record...", 5)
        
        ended_session = await conn.fetchrow(
            "UPDATE sessions SET ended_at = NOW(), pause_until = NOW() + make_interval(hours => $1) "
            "WHERE id = $2 AND ended_at IS NULL RETURNING *",
            pause_hours, session_id
        )
        if not ended_session:
            return None

    # 2. Process badges (MUST happen before resetting counts)
    async with pool.acquire() as conn:
        if progress_callback:
            await progress_callback("Fetching top uploaders...", 15)

        top_users = await conn.fetch(
            """SELECT * FROM users
               WHERE session_upload_count > 0
               ORDER BY session_upload_count DESC
               LIMIT $1""",
            leaderboard_top
        )

        badge_assignments = []
        num_top = len(top_users)
        for i, user in enumerate(top_users):
            rank = i + 1
            badge = badge_for_rank(rank)
            if badge:
                if progress_callback and i % 5 == 0:
                    pct = 15 + int((i / num_top) * 20)
                    await progress_callback(f"Distributing badges ({rank}/{num_top})...", pct)
                
                existing = user['badge_emoji'] or ''
                # Only add badge if not already present
                if badge not in existing:
                    new_badges = f"{existing},{badge}" if existing else badge
                    await conn.execute(
                        "UPDATE users SET badge_emoji = $1 WHERE id = $2",
                        new_badges, user['id']
                    )
                
                badge_assignments.append({
                    'user': dict(user),
                    'rank': rank,
                    'badge': badge
                })

    # 3. Reset upload counts IMMEDIATELY after badge processing
    async with pool.acquire() as conn:
        if progress_callback:
            await progress_callback("Resetting user stats...", 50)
        
        # Reset counts for everyone. This is critical for the leaderboard.
        await conn.execute("UPDATE users SET session_upload_count = 0 WHERE session_upload_count > 0")
        await asyncio.sleep(0.5)

    # 4. Update activity status in chunks
    async with pool.acquire() as conn:
        if progress_callback:
            await progress_callback("Updating activity status...", 70)

        # Identify top 10% of active users
        total_active = await conn.fetchval("SELECT COUNT(*) FROM users WHERE status = 'active'") or 0
        top_10_percent_limit = max(1, total_active // 10) if total_active > 0 else 0
        
        if progress_callback:
            await progress_callback("Updating activity status...", 85)

        # Reset status for all active users EXCEPT the top 10%
        if top_10_percent_limit > 0:
            await conn.execute(
                """UPDATE users SET status = 'inactive', uploads_since_inactive = 0 
                   WHERE status = 'active' AND id NOT IN (
                       SELECT id FROM users 
                       WHERE status = 'active'
                       ORDER BY total_media_lifetime DESC 
                       LIMIT $1
                   )""",
                top_10_percent_limit
            )
        else:
            await conn.execute(
                "UPDATE users SET status = 'inactive', uploads_since_inactive = 0 WHERE status = 'active'"
            )

        if progress_callback:
            await progress_callback("Finalizing session data...", 95)

        pause_until = ended_session['pause_until']

        return {
            'badge_assignments': badge_assignments,
            'ended_session': dict(ended_session),
            'pause_until': pause_until,
            'top_users': [dict(u) for u in top_users],
        }


async def cleanup_session_media(pool: asyncpg.Pool, session_id: int):
    """Background task to clean up media from an ended session in chunks."""
    try:
        logger.info(f"Starting media cleanup for session #{session_id}")
        async with pool.acquire() as conn:
            total_media = await conn.fetchval("SELECT COUNT(*) FROM media WHERE session_id = $1", session_id) or 0
            deleted_media = 0
            
            while True:
                # Delete in smaller batches of 200 to save CPU/IO on Nano instance
                res = await conn.execute(
                    "DELETE FROM media WHERE id IN (SELECT id FROM media WHERE session_id = $1 LIMIT 200)", 
                    session_id
                )
                count = int(res.split()[-1])
                if count == 0:
                    break
                deleted_media += count
                if deleted_media % 1000 == 0:
                    logger.info(f"Cleanup session #{session_id}: {deleted_media}/{total_media} media deleted")
                await asyncio.sleep(1.0) # 1s pause between batches for Nano health
            
            logger.info(f"Completed media cleanup for session #{session_id}. Total: {deleted_media}")
    except Exception as e:
        logger.error(f"Error in cleanup_session_media for session #{session_id}: {repr(e)}")


async def create_new_session(pool: asyncpg.Pool) -> asyncpg.Record:
    try:
        async with asyncio.timeout(15):
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute("UPDATE users SET session_upload_count = 0")
                    last = await conn.fetchrow("SELECT MAX(session_number) as max_num FROM sessions")
                    next_number = (last['max_num'] or 0) + 1
                    return await conn.fetchrow(
                        "INSERT INTO sessions (session_number) VALUES ($1) RETURNING *",
                        next_number
                    )
    except Exception as e:
        logger.error(f"Error creating new session: {repr(e)}")
        raise


async def get_session_stats(pool: asyncpg.Pool) -> dict:
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                session = await conn.fetchrow(
                    "SELECT * FROM sessions ORDER BY id DESC LIMIT 1"
                )
                total_uploads = 0
                if session:
                    total_uploads = await conn.fetchval(
                        "SELECT COUNT(*) FROM media WHERE session_id = $1", session['id']
                    ) or 0
                top_user = await conn.fetchrow(
                    "SELECT anonymous_name, session_upload_count FROM users "
                    "ORDER BY session_upload_count DESC LIMIT 1"
                )
                return {
                    'session': dict(session) if session else {},
                    'total_uploads': int(total_uploads),
                    'top_user': dict(top_user) if top_user else None,
                }
    except Exception as e:
        logger.error(f"Error fetching session stats: {repr(e)}")
        return {}


async def store_sent_message(
    pool: asyncpg.Pool,
    recipient_id: int,
    message_id: int,
    session_id: int,
    media_id: Optional[int] = None
):
    try:
        async with asyncio.timeout(10):
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO sent_messages (recipient_id, message_id, session_id, media_id) "
                    "VALUES ($1, $2, $3, $4)",
                    recipient_id, message_id, session_id, media_id
                )
    except Exception as e:
        logger.error(f"Error storing sent message: {repr(e)}")


async def store_sent_messages_batch(pool: asyncpg.Pool, batch: List[tuple]):
    if not batch:
        return
    try:
        async with asyncio.timeout(20):
            async with pool.acquire() as conn:
                await conn.executemany(
                    "INSERT INTO sent_messages (recipient_id, message_id, session_id, media_id) "
                    "VALUES ($1, $2, $3, $4)",
                    batch
                )
    except Exception as e:
        logger.error(f"Error storing sent messages batch: {repr(e)}")


async def get_session_sent_messages(pool: asyncpg.Pool, session_id: int) -> List[asyncpg.Record]:
    try:
        async with asyncio.timeout(15):
            async with pool.acquire() as conn:
                return await conn.fetch(
                    "SELECT recipient_id, message_id FROM sent_messages WHERE session_id = $1",
                    session_id
                )
    except Exception as e:
        logger.error(f"Error fetching session messages: {repr(e)}")
        return []


async def get_session_sent_messages_batch(
    pool: asyncpg.Pool, session_id: int, limit: int = 200, offset: int = 0
) -> List[asyncpg.Record]:
    try:
        async with asyncio.timeout(15):
            async with pool.acquire() as conn:
                return await conn.fetch(
                    "SELECT id, recipient_id, message_id FROM sent_messages "
                    "WHERE session_id = $1 ORDER BY id ASC LIMIT $2 OFFSET $3",
                    session_id, limit, offset
                )
    except Exception as e:
        logger.error(f"Error fetching session messages batch: {repr(e)}")
        return []


async def delete_sent_messages_batch(pool: asyncpg.Pool, ids: List[int]):
    if not ids:
        return
    try:
        async with asyncio.timeout(15):
            async with pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM sent_messages WHERE id = ANY($1::int[])",
                    ids
                )
    except Exception as e:
        logger.error(f"Error deleting messages batch: {repr(e)}")


async def clear_sent_messages(pool: asyncpg.Pool, session_id: int):
    try:
        async with asyncio.timeout(20):
            async with pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM sent_messages WHERE session_id = $1",
                    session_id
                )
    except Exception as e:
        logger.error(f"Error clearing session messages: {repr(e)}")


async def get_wipe_stats(pool: asyncpg.Pool) -> dict:
    try:
        async with asyncio.timeout(10): # 10 second timeout
            async with pool.acquire() as conn:
                total_messages = await conn.fetchval(
                    "SELECT COUNT(*) FROM sent_messages"
                ) or 0

                unique_recipients = await conn.fetchval(
                    "SELECT COUNT(DISTINCT recipient_id) FROM sent_messages"
                ) or 0

                unique_sessions = await conn.fetchval(
                    "SELECT COUNT(DISTINCT session_id) FROM sent_messages"
                ) or 0

                media_in_queue = await conn.fetchval(
                    "SELECT COUNT(*) FROM media WHERE sent_at IS NULL"
                ) or 0

                active_users = await conn.fetchval(
                    "SELECT COUNT(*) FROM users WHERE status = 'active' AND bot_blocked = FALSE"
                ) or 0

                total_users = await conn.fetchval(
                    "SELECT COUNT(*) FROM users WHERE status NOT IN ('banned') AND bot_blocked = FALSE"
                ) or 0

                return {
                    'total_messages': int(total_messages),
                    'unique_recipients': int(unique_recipients),
                    'unique_sessions': int(unique_sessions),
                    'media_in_queue': int(media_in_queue),
                    'active_users': int(active_users),
                    'total_users': int(total_users),
                    'status': 'ok'
                }
    except asyncio.TimeoutError:
        logger.error("Timeout fetching wipe stats from database")
        return {'status': 'timeout', 'error': 'Database timeout'}
    except Exception as e:
        logger.error(f"Error fetching wipe stats: {repr(e)}")
        return {'status': 'error', 'error': str(e)}


async def clear_all_sent_messages(pool: asyncpg.Pool):
    async with pool.acquire() as conn:
        # TRUNCATE is much faster and uses almost no Disk IO compared to DELETE
        await conn.execute("TRUNCATE TABLE sent_messages")


async def get_all_sent_messages_batch(
    pool: asyncpg.Pool, limit: int = 200
) -> List[asyncpg.Record]:
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT id, recipient_id, message_id FROM sent_messages "
            "ORDER BY id ASC LIMIT $1",
            limit
        )
