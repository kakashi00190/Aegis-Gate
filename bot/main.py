import asyncio
import logging
import os
import sys
import asyncpg
from datetime import datetime, timedelta

from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage

import config
from database import init_db
from handlers import start, media, commands, admin
from tasks.broadcast import process_broadcast_queue, sent_messages_logger_task
from tasks.inactivity import check_inactivity
from tasks.session import check_session_end
from tasks.cleanup import cleanup_stale_verifications_task

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


import json
from aiohttp import web, WSCloseCode

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
        from database import get_advanced_stats
        stats = await get_advanced_stats(pool)
        
        # Convert datetime objects to string for JSON serialization
        def json_serial(obj):
            if isinstance(obj, (datetime, timedelta)):
                return str(obj)
            return obj

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

async def run_health_server(pool):
    port = int(os.environ.get("PORT", 8080))
    app = web.Application()
    app['pool'] = pool
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
            from database import get_advanced_stats
            last_stats = None
            while True:
                health_monitor.update("stats_broadcast")
                await asyncio.sleep(15) # Broadcast every 15 seconds
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
                            logger.debug(f"Error sending to WS client: {e}")
                except Exception as e:
                    logger.error(f"Error in stats broadcast loop: {e}")

        asyncio.create_task(broadcast_stats())
        
    except OSError:
        logger.info(f"Health server skipped (port {port} already in use)")


async def main():
    logger.info("Starting Telegram Media Sharing Bot...")

    pool = await asyncpg.create_pool(
        config.DATABASE_URL,
        min_size=5,
        max_size=50,
        command_timeout=60,
        statement_cache_size=0
    )
    logger.info("Database pool created.")

    await init_db(pool)
    logger.info("Database initialized.")

    bot = Bot(
        token=config.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )

    dp = Dispatcher(storage=MemoryStorage())
    dp["pool"] = pool

    dp.include_router(start.router)
    dp.include_router(media.router)
    dp.include_router(commands.router)
    dp.include_router(admin.router)

    loop = asyncio.get_running_loop()
    loop.create_task(run_health_server(pool))
    loop.create_task(process_broadcast_queue(bot, pool))
    loop.create_task(sent_messages_logger_task(pool))
    loop.create_task(check_inactivity(bot, pool))
    loop.create_task(check_session_end(bot, pool))
    loop.create_task(cleanup_stale_verifications_task(pool))

    logger.info(f"Bot running. Admin ID: {config.ADMIN_ID}")

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await pool.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
