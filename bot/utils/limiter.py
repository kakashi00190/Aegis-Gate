import asyncio
import time
import random
import logging

logger = logging.getLogger(__name__)

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
        """Called when FloodWait/429 is received — dynamically increase slowdown."""
        self._slowdown = min(self._slowdown * 1.3, 5.0)
        self._last_flood = time.monotonic()
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
                    if self.wait_count % 50 == 0:
                        logger.warning(f"Rate limiter threshold reached ({self.wait_count} waits). Slowing down requests to stay below {self.rate} req/sec.")
                    # Apply dynamic slowdown with jitter
                    base_wait = 1 / self.rate
                    wait = base_wait * self._slowdown * random.uniform(0.8, 1.3)
                    await asyncio.sleep(wait)
            self.tokens -= 1

# Telegram allows ~30 messages per second to different users
# We use 20 — leaves room for handler responses while dramatically
# improving broadcast throughput (was 8, too slow for 180+ recipients)
global_rate_limiter = TokenBucketLimiter(rate=20, capacity=30)
