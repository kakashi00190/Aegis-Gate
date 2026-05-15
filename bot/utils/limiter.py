import asyncio
import time
import random
import logging

logger = logging.getLogger(__name__)

class BanWaveDetector:
    """Detects Telegram ban waves from FloodWait patterns and triggers tiered auto-dodge"""
    
    def __init__(self):
        self.floodwait_history = []  # List of (timestamp, duration)
        self.ban_wave_detected = False
        self.dodge_until = None  # When dodge period ends
        self.recovery_until = None  # When recovery period ends
        self.detection_window = 300  # seconds to analyze (5 mins)
        self._bot = None  # Bot instance for notifications (set at startup)
        
        # Scoring thresholds
        self.SCORE_MODERATE = 10
        self.SCORE_SEVERE = 30
    
    def set_bot(self, bot):
        """Set the bot instance for admin notifications"""
        self._bot = bot
    
    def _calculate_score(self, now: float) -> float:
        """Calculate current violation score based on recent FloodWaits"""
        cutoff = now - self.detection_window
        # Prune old history
        self.floodwait_history = [item for item in self.floodwait_history if item[0] > cutoff]
        
        score = 0.0
        for _, duration in self.floodwait_history:
            if duration < 10:
                score += 1
            elif duration < 60:
                score += 3
            elif duration < 3600:
                score += 10
            else:
                score += 30
        return score

    def record_floodwait(self, duration: float):
        """Record a FloodWait occurrence and check for ban wave severity"""
        now = time.monotonic()
        self.floodwait_history.append((now, duration))
        
        score = self._calculate_score(now)
        
        if score >= self.SCORE_SEVERE:
            severity = "SEVERE"
            dodge_sec = random.uniform(3600, 14400)  # 1-4 hours
        elif score >= self.SCORE_MODERATE:
            severity = "MODERATE"
            dodge_sec = random.uniform(900, 1800)    # 15-30 mins
        else:
            return False # Not enough score to trigger dodge

        if not self.ban_wave_detected:
            self.ban_wave_detected = True
            self.dodge_until = now + dodge_sec
            
            logger.warning(f"🛡️ {severity} BAN WAVE DETECTED! Score: {score:.1f}. Dodging for {dodge_sec:.0f}s")
            asyncio.create_task(self._notify_admin_ban_wave(score, dodge_sec, severity))
            return True
        return False
    
    async def _notify_admin_ban_wave(self, score: float, duration: float, severity: str):
        """Send admin-only notification about ban wave detection"""
        try:
            from config import ADMIN_IDS
            from datetime import datetime, timezone
            
            if not self._bot:
                return
            
            resume_time = datetime.now(timezone.utc).timestamp() + duration
            resume_str = datetime.fromtimestamp(resume_time, timezone.utc).strftime('%H:%M:%S UTC')
            
            for admin_id in ADMIN_IDS:
                try:
                    await self._bot.send_message(
                        admin_id,
                        f"🚨 <b>BAN WAVE DETECTED ({severity})</b>\n\n"
                        f"• Violation Score: {score:.1f}\n"
                        f"• Action: Auto-dodge activated\n"
                        f"• Duration: {duration/60:.1f} min\n"
                        f"• Estimated Resume: <code>{resume_str}</code>\n\n"
                        f"Bot is pausing all broadcasts to prevent a permanent ban.",
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.error(f"Failed to notify admin {admin_id}: {e}")
        except Exception as e:
            logger.error(f"Failed to notify admins about ban wave: {e}")
    
    def should_dodge(self) -> bool:
        """Check if we should pause broadcasts due to ban wave"""
        now = time.monotonic()
        if self.ban_wave_detected and self.dodge_until:
            if now < self.dodge_until:
                return True
            else:
                # Dodge period over, enter recovery mode
                self.ban_wave_detected = False
                self.recovery_until = now + random.uniform(1800, 3600) # 30-60m recovery
                self.dodge_until = None
                logger.info(f"🛡️ Ban wave dodge ended. Entering recovery mode until {self.recovery_until - now:.0f}s from now.")
                asyncio.create_task(self._notify_admin_recovery())
        return False

    def is_in_recovery(self) -> bool:
        """Check if bot is in post-dodge recovery mode (cautionary phase)"""
        if self.recovery_until:
            return time.monotonic() < self.recovery_until
        return False
    
    async def _notify_admin_recovery(self):
        """Send admin-only notification when ban wave subsides"""
        try:
            from config import ADMIN_IDS
            if not self._bot:
                return
            for admin_id in ADMIN_IDS:
                try:
                    await self._bot.send_message(
                        admin_id,
                        f"✅ <b>BAN WAVE SUBSIDED</b>\n\n"
                        f"• Auto-dodge completed\n"
                        f"• Now in <b>Recovery Mode</b> (Cautionary pacing)\n"
                        f"• Monitoring for new patterns\n\n"
                        f"Bot is gradually resuming operations.",
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.error(f"Failed to notify admin {admin_id} about recovery: {e}")
        except Exception as e:
            logger.error(f"Failed to notify admins about recovery: {e}")
    
    def get_status(self) -> dict:
        """Get current detector status for monitoring"""
        now = time.monotonic()
        return {
            'ban_wave_detected': self.ban_wave_detected,
            'dodge_remaining': max(0, (self.dodge_until or now) - now) if self.dodge_until else 0,
            'recovery_active': self.is_in_recovery(),
            'current_score': self._calculate_score(now),
            'detection_window': self.detection_window
        }

# Global ban wave detector
ban_wave_detector = BanWaveDetector()

class TokenBucketLimiter:
    def __init__(self, rate: int, capacity: int):
        self.rate = rate  # tokens per second
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.monotonic()
        self.lock = asyncio.Lock()
        self.wait_count = 0
        # Dynamic slowdown: multiplier increases on FloodWait, decays over time
        self._slowdown = 1.0
        self._last_flood = 0.0
        # Per-user send tracking — prevents sending too fast to one user
        self._user_last_send: dict[int, float] = {}
        self._user_min_interval = 0.5  # Min 0.5s between sends to same user (Telegram per-chat limit ~1/sec)

    @property
    def slowdown(self) -> float:
        return self._slowdown

    def apply_flood_pressure(self):
        """Called when FloodWait/429 is received — dynamically increase slowdown.
        Uses gentle 1.15x multiplier (no cooldown) so every FloodWait compounds
        safely: 1.15^5 = 2.01x vs old 1.3^5 = 3.71x. Capped at 5.0x."""
        now = time.monotonic()
        self._slowdown = min(self._slowdown * 1.15, 5.0)
        self._last_flood = now
        logger.warning(f"Dynamic slowdown increased to {self._slowdown:.2f}x due to flood pressure")

    def _recover_slowdown(self):
        """Slowly recover slowdown over time when no floods occur."""
        now = time.monotonic()
        if self._slowdown > 1.0 and now - self._last_flood > 30:
            self._slowdown = max(1.0, self._slowdown * 0.95)
            if self._slowdown < 1.05:
                self._slowdown = 1.0

    async def consume(self, priority: bool = False):
        """Consume a token. If priority=True, reserve 1 token for interactive/user-facing sends.
        Broadcast tasks should NOT use priority — only handlers responding to user actions."""
        self._recover_slowdown()
        while True:
            async with self.lock:
                now = time.monotonic()
                elapsed = now - self.last_update
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.last_update = now
                # Priority: can take the last reserved token (keeps 1 token for interactive)
                available = self.tokens if priority else max(0, self.tokens - 1)
                if available >= 1:
                    self.tokens -= 1
                    self.wait_count = 0
                    return
                # Not enough tokens — calculate wait time UNDER lock, then sleep OUTSIDE
                self.wait_count += 1
                if self.wait_count == 50 or (self.wait_count > 50 and self.wait_count % 200 == 0):
                    logger.warning(f"Rate limiter pacing ({self.wait_count} waits, slowdown={self._slowdown:.2f}x). Staying below {self.rate} req/sec.")
                base_wait = 1 / self.rate
                wait = base_wait * self._slowdown * random.uniform(0.8, 1.3)
            # Sleep OUTSIDE the lock — other tasks can proceed concurrently
            await asyncio.sleep(wait)

    async def consume_bulk(self, count: int):
        """Consume multiple tokens at once (for send_media_group accounting).
        Instead of N sequential consume() calls (each waiting 0.2s = N×0.2s total),
        this drains N tokens from the bucket in one wait cycle."""
        self._recover_slowdown()
        while True:
            async with self.lock:
                now = time.monotonic()
                elapsed = now - self.last_update
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.last_update = now
                if self.tokens >= count:
                    self.tokens -= count
                    self.wait_count = 0
                    return
                # Not enough tokens — calculate how long to wait for all of them
                deficit = count - self.tokens
                wait = (deficit / self.rate) * self._slowdown * random.uniform(0.8, 1.3)
            # Sleep OUTSIDE the lock
            await asyncio.sleep(wait)

    async def consume_for_user(self, user_id: int, priority: bool = False):
        """Consume a token AND enforce per-user minimum interval.
        Use this for sends directed at a specific user (broadcasts, send-backs, etc).
        Prevents sending too fast to one user — Telegram enforces per-chat limits.
        If priority=True, this is an interactive/user-facing send (handler response)
        and should get a token even when the bucket is nearly empty."""
        # First, get a global token
        await self.consume(priority=priority)
        # Then enforce per-user interval
        now = time.monotonic()
        last = self._user_last_send.get(user_id, 0)
        wait = self._user_min_interval - (now - last)
        if wait > 0:
            await asyncio.sleep(wait)
        self._user_last_send[user_id] = time.monotonic()
        # Periodic cleanup of stale user tracking (keep last 1000)
        if len(self._user_last_send) > 1000:
            cutoff = now - 60  # Remove entries older than 60s
            self._user_last_send = {
                uid: ts for uid, ts in self._user_last_send.items()
                if ts > cutoff
            }

# Telegram allows ~30 messages per second to different users (BURST)
# Sustained 24/7 sending must be MUCH lower to avoid violations.
# 15 → violated. 10 → violated. 5 → violated (caused by lock contention + delete_message stealing tokens, now fixed).
# 7 → testing with all fixes applied (lock-free sleep, priority tokens, delete separation).
# 3 is the safe fallback for NORMAL mode.
global_rate_limiter = TokenBucketLimiter(rate=3, capacity=6)  # capacity=2×rate for burst headroom
