"""
Upstash Redis REST client singleton.

Usage:
    from utils.redis_client import redis, redis_ping

    await redis_ping()              # health check
    await redis.set("key", "val")   # normal usage
    await redis.get("key")
    await redis.incr("counter")
    await redis.expire("key", 3600)

If UPSTASH_REDIS_REST_URL / TOKEN are not set, `redis` will be None and
redis_ping() will return False — the bot will still start and operate
normally without Redis (Supabase queue is used as fallback).
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from upstash_redis.asyncio import Redis as UpstashRedis
    _UPSTASH_AVAILABLE = True
except ImportError:
    _UPSTASH_AVAILABLE = False
    logger.warning("upstash-redis not installed. Redis features disabled.")


def _create_client() -> Optional[object]:
    """Create the Upstash Redis client from environment variables.
    Returns None if credentials are missing or library is unavailable."""
    if not _UPSTASH_AVAILABLE:
        return None

    try:
        import config
        url = getattr(config, "UPSTASH_REDIS_REST_URL", None)
        token = getattr(config, "UPSTASH_REDIS_REST_TOKEN", None)

        if not url or not token:
            logger.warning(
                "UPSTASH_REDIS_REST_URL or UPSTASH_REDIS_REST_TOKEN not set. "
                "Redis disabled — bot will fall back to Supabase queue."
            )
            return None

        client = UpstashRedis(url=url, token=token)
        logger.info("✅ Upstash Redis client created successfully.")
        return client
    except Exception as e:
        logger.error(f"Failed to create Upstash Redis client: {e}")
        return None


# Module-level singleton — None if not configured
redis: Optional[object] = _create_client()


async def redis_ping() -> bool:
    """
    Ping the Redis server to verify connectivity.
    Returns True on success, False on failure or if Redis is not configured.
    """
    if redis is None:
        logger.warning("Redis ping skipped — client not configured.")
        return False

    try:
        result = await redis.ping()
        if result:
            logger.info("✅ Upstash Redis PING successful — connection is live.")
            return True
        else:
            logger.warning("⚠️ Upstash Redis PING returned unexpected result.")
            return False
    except Exception as e:
        logger.error(f"❌ Upstash Redis PING failed: {e}")
        return False
