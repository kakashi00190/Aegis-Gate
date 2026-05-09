import asyncio
import time
import random
import logging

logger = logging.getLogger(__name__)

class BanWaveDetector:
    """Detects Telegram ban waves from FloodWait patterns and triggers auto-dodge"""
    
    def __init__(self):
        self.floodwait_times = []  # Recent FloodWait timestamps
        self.floodwait_durations = []  # Recent FloodWait durations
        self.ban_wave_detected = False
        self.dodge_until = None  # When dodge period ends
        self.detection_window = 60  # seconds to analyze
        self.detection_threshold = 3  # FloodWaits in window to trigger
        self.high_duration_threshold = 30  # Any FloodWait > 30s is suspicious
        
    def record_floodwait(self, duration: float):
        """Record a FloodWait occurrence and check for ban wave"""
        now = time.monotonic()
        self.floodwait_times.append(now)
        self.floodwait_durations.append(duration)
        
        # Keep only recent data
        cutoff = now - self.detection_window
        self.floodwait_times = [t for t in self.floodwait_times if t > cutoff]
        self.floodwait_durations = self.floodwait_durations[-len(self.floodwait_times):]
        
        # Check for ban wave patterns
        recent_count = len(self.floodwait_times)
        has_long_wait = any(d > self.high_duration_threshold for d in self.floodwait_durations)
        
        if recent_count >= self.detection_threshold or has_long_wait:
            if not self.ban_wave_detected:
                self.ban_wave_detected = True
                self.dodge_until = now + random.uniform(300, 600)  # 5-10 min dodge
                logger.warning(f" BAN WAVE DETECTED! {recent_count} FloodWaits in {self.detection_window}s. Dodging for {self.dodge_until - now:.0f}s")
                return True
        return False
    
    def should_dodge(self) -> bool:
        """Check if we should pause broadcasts due to ban wave"""
        if self.ban_wave_detected and self.dodge_until:
            if time.monotonic() < self.dodge_until:
                return True
            else:
                # Dodge period over, reset
                self.ban_wave_detected = False
                self.dodge_until = None
                self.floodwait_times = []
                self.floodwait_durations = []
                logger.info(" Ban wave subsided. Resuming normal operations.")
        return False
    
    def get_status(self) -> dict:
        """Get current detector status for monitoring"""
        now = time.monotonic()
        recent_count = len([t for t in self.floodwait_times if t > now - self.detection_window])
        return {
            'ban_wave_detected': self.ban_wave_detected,
            'dodge_remaining': max(0, (self.dodge_until or now) - now) if self.dodge_until else 0,
            'recent_floodwaits': recent_count,
            'detection_threshold': self.detection_threshold
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

    @property
    def slowdown(self) -> float:
        return self._slowdown

    def apply_flood_pressure(self):
        """Called when FloodWait/429 is received — dynamically increase slowdown.
        Cooldown: only increase once per 10s to prevent concurrent FloodWait
        errors from stacking exponentially (1.3x → 1.69x → 2.2x → 3.71x)."""
        now = time.monotonic()
        if now - self._last_flood < 10:
            # Already applied pressure recently — just update timestamp
            self._last_flood = now
            return
        self._slowdown = min(self._slowdown * 1.3, 5.0)
        self._last_flood = now
        logger.warning(f"Dynamic slowdown increased to {self._slowdown:.2f}x due to flood pressure")

    def _recover_slowdown(self):
        """Slowly recover slowdown over time when no floods occur."""
        now = time.monotonic()
        if self._slowdown > 1.0 and now - self._last_flood > 30:
            self._slowdown = max(1.0, self._slowdown * 0.95)
            if self._slowdown < 1.05:
                self._slowdown = 1.0

    async def consume(self):
        self._recover_slowdown()
        async with self.lock:
            while self.tokens < 1:
                now = time.monotonic()
                elapsed = now - self.last_update
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.last_update = now
                if self.tokens < 1:
                    self.wait_count += 1
                    # Log at 50, then every 200 — avoids spam at high throughput
                    if self.wait_count == 50 or (self.wait_count > 50 and self.wait_count % 200 == 0):
                        logger.warning(f"Rate limiter pacing ({self.wait_count} waits, slowdown={self._slowdown:.2f}x). Staying below {self.rate} req/sec.")
                    # Apply dynamic slowdown with jitter
                    base_wait = 1 / self.rate
                    wait = base_wait * self._slowdown * random.uniform(0.8, 1.3)
                    await asyncio.sleep(wait)
            self.tokens -= 1
            # Reset wait counter after successful consume — prevents unbounded growth
            self.wait_count = 0

# Telegram allows ~30 messages per second to different users
# We use 15 — conservative but stable. 20+ caused FloodWaits.
# Lower concurrency (5 photo) + lower rate = no FloodWait cascades.
global_rate_limiter = TokenBucketLimiter(rate=15, capacity=15)
