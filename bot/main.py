import asyncio
import logging
import os
import sys
import time
import asyncpg
from datetime import datetime, timedelta

from aiohttp import web
import json
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramRetryAfter
from aiogram.types import CallbackQuery

import config
from validate_env import main as validate_environment
from database import init_db
from handlers import start, media, commands, admin, cleanup
from tasks.broadcast import process_broadcast_queue, sent_messages_logger_task
from tasks.inactivity import check_inactivity
from handlers.cleanup import auto_cleanup_duplicates_task
from utils.helpers import safe_error
from utils.callback_guard import is_callback_spam
from tasks.session import check_session_end
from tasks.cleanup import cleanup_stale_verifications_task, cleanup_48hr_media_task
from utils.redis_client import redis_ping as check_redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


from utils.health import TaskHealth

# Global list of active websocket connections
ws_clients = []

async def health_handler(request):
    """Basic health check for Render/Uptime monitors."""
    return web.Response(text="ok", status=200)

async def detailed_health_handler(request):
    """Detailed health check for internal monitoring."""
    status = TaskHealth.get_status()
    is_healthy = all(task["healthy"] for task in status.values())
    
    response_data = {
        "status": "healthy" if is_healthy else "degraded",
        "tasks": status,
        "timestamp": time.time()
    }
    
    return web.json_response(
        response_data, 
        status=200 if is_healthy else 503
    )

async def stats_ws_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    ws_clients.append(ws)
    logger.info(f"New WebSocket connection. Total clients: {len(ws_clients)}")
    
    try:
        # Send initial stats
        pool = request.app['pool']

        # Convert datetime objects to string for JSON serialization
        def json_serial(obj):
            if isinstance(obj, (datetime, timedelta)):
                return str(obj)
            return obj

        if pool is None:
            await ws.send_str(json.dumps({"status": "initializing", "message": "Database not ready yet"}, default=json_serial))
        else:
            from database import get_advanced_stats
            stats = await get_advanced_stats(pool)
            await ws.send_str(json.dumps(stats, default=json_serial))
        
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                if msg.data == 'close':
                    await ws.close()
            elif msg.type == web.WSMsgType.ERROR:
                logger.error(f'WebSocket connection closed with exception {ws.exception()}')
    finally:
        ws_clients.remove(ws)
        logger.info(f"WebSocket connection closed. Total clients: {len(ws_clients)}")
    
    return ws

# Module-level reference to the health server app so we can update pool later
_health_app = None

async def run_health_server(pool, port=None):
    global _health_app
    if port is None:
        port = int(os.environ.get("PORT", 8080))
    app = web.Application()
    app['pool'] = pool
    _health_app = app
    app.router.add_get("/", health_handler) # Handle root for Render's default health check
    app.router.add_get("/api/healthz", health_handler)
    app.router.add_get("/healthz", health_handler)
    app.router.add_get("/api/health", detailed_health_handler)
    app.router.add_get("/api/stats/ws", stats_ws_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    try:
        await site.start()
        logger.info(f"Health server running on port {port}")
        
        # Background task to broadcast stats updates
        async def broadcast_stats():
            if not pool:
                return # Skip if no pool provided
            from database import get_advanced_stats
            last_stats = None
            while True:
                TaskHealth.update("stats_broadcast")
                await asyncio.sleep(120) # Increased from 60s to 120s to save Disk IO on unhealthy DB
                if not ws_clients:
                    continue
                
                try:
                    stats = await get_advanced_stats(pool)
                    
                    # Only broadcast if stats changed to save bandwidth
                    if last_stats == stats:
                        continue
                    last_stats = stats

                    def json_serial(obj):
                        if hasattr(obj, 'isoformat'):
                            return obj.isoformat()
                        return str(obj)
                    
                    data = json.dumps(stats, default=json_serial)
                    for ws in ws_clients:
                        try:
                            if not ws.closed:
                                await ws.send_str(data)
                        except Exception as e:
                            logger.debug(f"Error sending to WS client: {safe_error(e)}")
                except Exception as e:
                    logger.error(f"Error in stats broadcast loop: {safe_error(e)}")

        asyncio.create_task(broadcast_stats())
        
    except OSError:
        logger.info(f"Health server skipped (port {port} already in use)")


async def main():
    # 1. Start Health Server IMMEDIATELY (Render requires this)
    port = int(os.environ.get("PORT", 8080))
    # Start with pool=None; we update app['pool'] once real pool is ready
    loop = asyncio.get_running_loop()
    loop.create_task(run_health_server(None, port))
    
    # 2. Wait for potential old instances to shut down on Render
    await asyncio.sleep(5)
    
    logger.info("Starting Telegram Media Sharing Bot...")

    # Validate environment before starting
    if not await validate_environment():
        logger.error("🛑 Startup aborted due to validation failure.")
        return

    # Start health server early
    # (Actually we already started a dummy one, so we just let it be or restart it)
    # The dummy server doesn't have the real pool, so detailed stats won't work yet
    # but the health check will pass. We'll replace it once we have the pool.
    # For now, just continue.
    
    # 5. Initialize Database Pool
    pool = await asyncpg.create_pool(
        config.DATABASE_URL,
        min_size=1, # Minimum connections for Supabase Nano
        max_size=10, # Further reduced for stability on resource-constrained Nano
        command_timeout=300, # Increased to 5 mins to handle slow migrations/queries
        statement_cache_size=0,
        max_inactive_connection_lifetime=300.0,
        max_queries=500 # Reduced to prevent memory issues on small instances
    )
    logger.info("Database pool created.")

    # Update the health server's pool reference now that we have a real pool
    # This avoids starting a second server on the same port (which would fail)
    if _health_app is not None:
        _health_app['pool'] = pool
        logger.info("Health server pool updated with real database connection.")
    else:
        logger.warning("Health app not initialized yet — pool update deferred.")

    # Run DB init BEFORE starting the bot tasks
    # This ensures migrations are done before any queries run
    logger.info("Starting database initialization...")
    await init_db(pool)
    logger.info("Database initialization complete.")

    # Check Redis connectivity
    redis_ok = await check_redis()
    if redis_ok:
        logger.info("✅ Upstash Redis is connected and healthy.")
    else:
        logger.warning("⚠️ Upstash Redis is NOT connected. Bot will run using Supabase only.")

    bot = Bot(
        token=config.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )

    dp = Dispatcher(storage=MemoryStorage())
    dp["pool"] = pool

    # Callback spam protection middleware — blocks rapid-fire duplicate callbacks under 0.7s
    # Must be middleware (not handler) so it doesn't consume the event
    from aiogram import BaseMiddleware
    class CallbackSpamMiddleware(BaseMiddleware):
        async def __call__(self, handler, event, data):
            if isinstance(event, CallbackQuery) and event.data:
                if is_callback_spam(event.from_user.id, event.data):
                    await event.answer("⏳ Slow down!", show_alert=False)
                    return  # block — don't call next handler
            return await handler(event, data)

    dp.callback_query.middleware(CallbackSpamMiddleware())

    # Global error handler for flood control — prevents unhandled TelegramRetryAfter crashes
    @dp.error()
    async def on_error(event):
        exception = event.exception
        if isinstance(exception, TelegramRetryAfter):
            logger.warning(f"Global flood control caught: retry after {exception.retry_after}s. Ignoring.")
            # Record for ban wave detection even if caught globally
            ban_wave_detector.record_floodwait(exception.retry_after)
            return True
        logger.error(f"Unhandled error in dispatcher: {safe_error(exception)}")
        return True

    dp.include_router(start.router)
    dp.include_router(media.router)
    dp.include_router(commands.router)
    dp.include_router(admin.router)
    dp.include_router(cleanup.router)

    # Wire bot instance to ban wave detector for admin notifications
    from utils.limiter import ban_wave_detector
    ban_wave_detector.set_bot(bot)

    loop = asyncio.get_running_loop()
    loop.create_task(process_broadcast_queue(bot, pool))
    loop.create_task(sent_messages_logger_task(pool))
    loop.create_task(check_inactivity(bot, pool))
    loop.create_task(check_session_end(bot, pool))
    loop.create_task(cleanup_stale_verifications_task(pool))
    loop.create_task(cleanup_48hr_media_task(pool))
    loop.create_task(auto_cleanup_duplicates_task(bot, pool))

    logger.info(f"Bot running. Admin IDs configured: {len(config.ADMIN_IDS)}")

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        logger.error(f"Polling error: {safe_error(e)}")
    finally:
        logger.info("Closing bot resources...")
        await bot.session.close()
        await pool.close()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
