import asyncio
import time
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

    async def consume(self):
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
                    await asyncio.sleep(1 / self.rate)
            self.tokens -= 1

# Telegram allows ~30 messages per second to different users
# We use 25 for safety
global_rate_limiter = TokenBucketLimiter(rate=25, capacity=30)
