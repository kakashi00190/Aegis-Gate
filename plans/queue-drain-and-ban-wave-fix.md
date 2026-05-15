# Queue Drain & Ban Wave Prevention — Refined Fix Plan

## Deep Analysis Summary

### The Core Problem

The bot has a fundamental tension: **high user count × sequential broadcast = slow drain**, but **increasing rate = Telegram violations**. The adaptive throughput system is self-defeating:

```
High backlog → Higher rate → Violation → Lower rate → Higher backlog → (loop)
```

### Why Ban Wave Detection Fails

| # | Failure Mode | Location | Impact |
|---|-------------|----------|--------|
| 1 | Handler FloodWaits never recorded | [`_safe_answer`](Aegis-Gate-main/bot/handlers/media.py:158), [`_safe_send`](Aegis-Gate-main/bot/handlers/media.py:176) | Detector is blind to handler-caused violations |
| 2 | `apply_flood_pressure` has 10s cooldown | [`limiter.py:180`](Aegis-Gate-main/bot/utils/limiter.py:180) | Concurrent FloodWaits don't compound — only first one counts |
| 3 | Adaptive modes go up to 10/sec | [`broadcast.py:478`](Aegis-Gate-main/bot/tasks/broadcast.py:478) | Historical data shows even 5/sec violated; 10/sec guarantees it |
| 4 | Recovery mode at 40% of 10/sec = 4/sec | [`broadcast.py:505`](Aegis-Gate-main/bot/tasks/broadcast.py:505) | Still near violation threshold when base rate is 10/sec |
| 5 | `_safe_answer` bypasses rate limiter entirely | [`media.py:157`](Aegis-Gate-main/bot/handlers/media.py:157) | Handler responses consume API calls outside the token bucket |

### Throughput Math (Why Queue Never Drains)

With 1000 users at 4 msg/sec (NORMAL mode), one media item takes ~255 seconds. If users upload faster than 1 item per 255 seconds, the queue grows forever.

**The only way to drain faster without increasing rate: batch more items per API call.**

| Batch Size | API Calls (100 items × 1000 users) | Drain Time at 4/sec |
|-----------|-----------------------------------|---------------------|
| 1 (no batching) | 100,000 | 7.0 hours |
| 3 (current) | 33,000 | 2.3 hours |
| 5 (proposed) | 20,000 | 1.4 hours |

### Continuous vs Violate-Dodge Throughput

| Strategy | Effective Throughput |
|----------|---------------------|
| Current: 10/sec for ~2h → 4h dodge → repeat | ~12,000 sends/hour avg |
| Proposed: 5/sec continuous (no dodge) | **18,000 sends/hour** (50% more) |
| Proposed: 3/sec continuous (no dodge) | 10,800 sends/hour (comparable, reliable) |

**Key insight:** Running continuously at a safe rate drains MORE queue than running fast and dodging.

---

## Fix Plan — 3 Phases (9 Fixes, Refined)

### Phase 1: Stop Invisible Violations (Critical)

**Goal:** Make the ban wave detector see ALL FloodWaits so it can react before a ban.

#### Fix 1.1 — Record FloodWaits in handler safe-send functions

**File:** [`bot/handlers/media.py`](Aegis-Gate-main/bot/handlers/media.py:154)

**Change:** In `_safe_answer` (line 158) and `_safe_send` (line 176), add `ban_wave_detector.record_floodwait(e.retry_after)` before `await asyncio.sleep(e.retry_after)`.

**Why:** These functions catch `TelegramRetryAfter` but never tell the detector. The global error handler at [`main.py:240`](Aegis-Gate-main/bot/main.py:240) only catches UNHANDLED exceptions — these handlers swallow the exception, so the global handler never sees them. Handler FloodWaits are completely invisible.

**Edge cases:** None. FloodWaits are real 429 responses from Telegram. Recording them is always correct. No false positive risk.

**Code location — `_safe_answer`:**
- Import needed: `from utils.limiter import ban_wave_detector` (add near line 16-17)
- Insert at line 159 (after `except TelegramRetryAfter as e:` and before `logger.warning(...)`)

**Code location — `_safe_send`:**
- Import already available via local import at line 172
- Insert at line 177 (after `except TelegramRetryAfter as e:` and before `logger.warning(...)`)

---

#### Fix 1.2 — Remove 10s cooldown on flood pressure, use gentler multiplier

**File:** [`bot/utils/limiter.py`](Aegis-Gate-main/bot/utils/limiter.py:175)

**Change:** In `apply_flood_pressure`:
- Remove the 10s cooldown guard (lines 180-183)
- Change multiplier from `1.3` to `1.15`
- Keep the 5.0× cap

**Why:** The 10s cooldown was added to prevent concurrent FloodWait errors from stacking exponentially (1.3^5 = 3.71×). But it also means the rate limiter ignores 4 out of 5 FloodWaits in a real flood. With 1.15×, 5 consecutive FloodWaits = 2.01× — controlled and safe. And after Fix 3.3 (parallel broadcast), concurrent FloodWaits become more likely, making the cooldown even more harmful.

**Important:** `apply_flood_pressure` is ONLY called from [`broadcast.py:263`](Aegis-Gate-main/bot/tasks/broadcast.py:263) (inside `send_media_to_user`). It is NOT called from handlers. So this only affects broadcast-caused FloodWaits.

**Before:**
```python
def apply_flood_pressure(self):
    now = time.monotonic()
    if now - self._last_flood < 10:
        self._last_flood = now
        return
    self._slowdown = min(self._slowdown * 1.3, 5.0)
    self._last_flood = now
```

**After:**
```python
def apply_flood_pressure(self):
    now = time.monotonic()
    self._slowdown = min(self._slowdown * 1.15, 5.0)
    self._last_flood = now
    logger.warning(f"Dynamic slowdown increased to {self._slowdown:.2f}x due to flood pressure")
```

---

#### Fix 1.3 — Route `_safe_answer` through rate limiter

**File:** [`bot/handlers/media.py`](Aegis-Gate-main/bot/handlers/media.py:154)

**Change:** Add `await global_rate_limiter.consume_for_user(message.chat.id, priority=True)` before `await message.answer(text, **kwargs)` at line 157.

**Why:** `_safe_answer` currently calls `message.answer()` with zero rate limiting. During high upload activity, each upload triggers 1-3 `_safe_answer` calls (delete + progress + confirmation). With 100 active users, that's 100-300 API calls the rate limiter doesn't know about. The bot thinks it's idle while actually hammering Telegram.

**Edge cases:**
- `priority=True` reserves 1 token for handler responses — they always get through even when broadcasts are saturating the bucket
- Per-user interval (0.5s) will naturally space multiple `_safe_answer` calls to the same user
- Import already available at line 16-17 area (add `global_rate_limiter` to existing limiter import)

---

### Phase 2: Safe Rate Calibration (Critical)

**Goal:** Cap rates at levels proven safe, with meaningful differentiation between modes.

#### Fix 2.1 — Lower adaptive mode maximums with tiered rates

**File:** [`bot/tasks/broadcast.py`](Aegis-Gate-main/bot/tasks/broadcast.py:477)

**Change:** Replace the rate table at lines 477-500:

| Mode | Old Rate | New Rate | Rationale |
|------|---------|----------|-----------|
| NORMAL | 4/sec | **3/sec** | Proven safe baseline (matches global default at [`limiter.py:270`](Aegis-Gate-main/bot/utils/limiter.py:270)) |
| HIGH | 6/sec | **4/sec** | Current NORMAL rate, already proven in production |
| CRITICAL | 8/sec | **5/sec** | Maximum safe ceiling based on historical data |
| EMERGENCY | 10/sec | **5/sec** | Same ceiling as CRITICAL, but with minimal gaps |

**Why tiered instead of all-at-5:** If all modes use the same rate, the modes become meaningless — gap timing (0.7s vs 20s) is negligible compared to broadcast_item execution time (30-200 seconds with many users). Tiered rates preserve meaningful differentiation: EMERGENCY is 67% faster than NORMAL (5 vs 3/sec).

**Why 5/sec is the ceiling:** The historical comment at [`limiter.py:267-269`](Aegis-Gate-main/bot/utils/limiter.py:267) documents: "15 → violated. 10 → violated. 5 → violated (caused by lock contention + delete_message stealing tokens, now fixed). 7 → testing." The lock contention and delete_message issues have been fixed, making 5/sec potentially safe now. But 7/sec is still untested. 5/sec is the responsible ceiling.

**Code changes (lines 477-500):**
```python
# OLD:
if backlog_count >= BACKLOG_EMERGENCY_THRESHOLD:
    effective_rate = 10
    ...
elif backlog_count >= BACKLOG_CRITICAL_THRESHOLD:
    effective_rate = 8
    ...
elif backlog_count >= BACKLOG_HIGH_THRESHOLD:
    effective_rate = 6
    ...
else:
    effective_rate = 4
    ...

# NEW:
if backlog_count >= BACKLOG_EMERGENCY_THRESHOLD:
    effective_rate = 5   # was 10
    ...
elif backlog_count >= BACKLOG_CRITICAL_THRESHOLD:
    effective_rate = 5   # was 8
    ...
elif backlog_count >= BACKLOG_HIGH_THRESHOLD:
    effective_rate = 4   # was 6
    ...
else:
    effective_rate = 3   # was 4
    ...
```

Also update the comment block at lines 465-468 to reflect new rates.

---

#### Fix 2.2 — Keep recovery multiplier at 0.4 (NO CHANGE from plan draft)

**File:** [`bot/tasks/broadcast.py`](Aegis-Gate-main/bot/tasks/broadcast.py:505)

**Decision: KEEP 0.4, do NOT change to 0.25.**

**Why the reversal:** The original concern was 0.4 × 10/sec = 4/sec during recovery — still near violation threshold. But with the new rate caps (max 5/sec), 0.4 × 5/sec = 2/sec. That's safely below threshold. Changing to 0.25 would give 0.75/sec in NORMAL recovery — one media item to 1000 users would take ~22 minutes. Unnecessarily slow.

The combination of lower base rates (Fix 2.1) + 0.4 recovery is already safe. No change needed here.

---

#### Fix 2.3 — Remove "human-like rest" random pauses

**File:** [`bot/tasks/broadcast.py`](Aegis-Gate-main/bot/tasks/broadcast.py:621)

**Change:** Delete lines 620-624 (the entire `if random.random() < 0.005:` block).

**Why:** The 0.5% chance of a 2-10 minute pause doesn't prevent violations — Telegram detects bots by API call rate, not by activity patterns. It only makes queue drain unpredictable and slower. Removing it is purely beneficial with no downside.

---

### Phase 3: Throughput Optimization (Important)

**Goal:** Drain queue faster without increasing API calls per second.

#### Fix 3.1 — Increase single-item batch size from 3→5

**File:** [`bot/tasks/broadcast.py`](Aegis-Gate-main/bot/tasks/broadcast.py:598)

**Change:** Line 598: `range(0, len(items), 3)` → `range(0, len(items), 5)`. Also update the comment on line 597.

**Why:** `send_media_group` is 1 API call regardless of 3 or 5 items. Telegram's limit is 10 per media group. The per-user interval (0.5s) already prevents rapid successive sends to the same user. This reduces `broadcast_item` calls for single items by 40%.

---

#### Fix 3.2 — Reduce proactive cooldown frequency

**File:** [`bot/tasks/broadcast.py`](Aegis-Gate-main/bot/tasks/broadcast.py:477)

**Change:** Update cooldown parameters alongside the rate changes in Fix 2.1:

| Mode | Old cooldown_n | New cooldown_n | Old gap | New gap |
|------|---------------|---------------|---------|---------|
| NORMAL | 15 | 20 | 8.5-20.5s | 5-12s |
| HIGH | 30 | 30 | 3.5-8.5s | 3-8s |
| CRITICAL | 50 | 40 | 1.2-3.2s | 1-4s |
| EMERGENCY | 60 | 50 | 0.7-1.7s | 0.5-2s |

**Why:** With the rate limiter capping at 3-5/sec, cooldowns are partially redundant for rate control. But they still serve a purpose — creating activity gaps that break up continuous sending patterns (pattern diversity). The reduced frequency means less overhead while keeping pattern-breaking gaps.

---

#### Fix 3.3 — Parallelize broadcast_item calls (2 concurrent via bounded queue)

**File:** [`bot/tasks/broadcast.py`](Aegis-Gate-main/bot/tasks/broadcast.py:605)

**Change:** Replace the sequential loop at lines 605-629 with a bounded-queue worker pattern:

```python
# OLD (sequential):
for items in grouped_media.values():
    broadcasts_since_cooldown += 1
    if broadcasts_since_cooldown >= cooldown_n:
        ...
    await broadcast_item(bot, pool, items, recipients, default_semaphore)
    # human-like rest (removed in Fix 2.3)
    gap = random.uniform(*gap_range)
    await asyncio.sleep(get_human_delay(gap))

# NEW (parallel with bounded queue):
async def _broadcast_worker(queue, worker_id):
    while True:
        items = await queue.get()
        if items is None:
            break
        try:
            await broadcast_item(bot, pool, items, recipients, default_semaphore)
        except Exception as e:
            logger.error(f"Worker {worker_id} broadcast error: {safe_error(e)}")
        finally:
            queue.task_done()
        # Gap after each item completes (rate limiter handles pacing during item)
        gap = random.uniform(*gap_range)
        await asyncio.sleep(get_human_delay(gap))

queue = asyncio.Queue(maxsize=2)
workers = [asyncio.create_task(_broadcast_worker(queue, i)) for i in range(2)]

for items in grouped_media.values():
    broadcasts_since_cooldown += 1
    if broadcasts_since_cooldown >= cooldown_n:
        cooldown = random.uniform(*cooldown_dur)
        logger.info(f"Proactive cooldown: {cooldown:.0f}s after {cooldown_n} broadcasts ({mode} mode)")
        await asyncio.sleep(cooldown)
        broadcasts_since_cooldown = 0
    await queue.put(items)  # Blocks if 2 items already processing

await queue.join()
for _ in range(2):
    await queue.put(None)
await asyncio.gather(*workers)
```

**Why this pattern instead of `asyncio.Semaphore(2)` + `asyncio.gather`:**
- Bounded queue (`maxsize=2`) naturally limits concurrency — the producer blocks when 2 items are processing
- Workers apply gaps AFTER each item completes, maintaining pattern diversity
- Clean shutdown with sentinel `None` values
- Each worker handles its own errors without crashing the other

**Edge cases addressed:**
- **Database pool:** Pool max is 10 ([`main.py:190`](Aegis-Gate-main/bot/main.py:190)). Two concurrent `broadcast_item` calls each use 1-2 connections for `mark_media_sent`. Plus health server + background tasks. Stays within 10.
- **Per-user interval:** If two items target the same user, `consume_for_user`'s 0.5s interval naturally serializes them.
- **Duplicate send protection:** `_check_duplicate_send` at [`broadcast.py:94`](Aegis-Gate-main/bot/tasks/broadcast.py:94) uses `(user_id, media_id)` key. Different media = different keys. No false duplicates.
- **Rate limiter:** Both workers share the same `global_rate_limiter`. Total API rate stays at the configured cap (3-5/sec).

**Why 2 workers (not 3 or 4):**
- Each worker processes a broadcast_item that may take 30-200 seconds
- 2 workers means at most 2× CHUNK_SIZE (40) concurrent send tasks
- More workers = more memory, more DB connections, more lock contention on rate limiter
- 2 is the sweet spot: doubles throughput for multi-item queues without resource exhaustion

---

## What We Are NOT Doing (And Why)

| Rejected Idea | Reason |
|--------------|--------|
| Longer dodge duration | Queue grows infinitely during dodge; prevention is better than punishment |
| Increase global rate above 5/sec | Historical data proves 5+ causes violations |
| User sharding (send to subsets) | Changes core UX; users expect to receive all content |
| Remove rate limiter entirely | Guaranteed instant ban |
| Recovery multiplier 0.25 | Unnecessarily slow with new rate caps; 0.4 × 5/sec = 2/sec is already safe |
| All modes at same rate (5/sec) | Modes become meaningless; tiered rates (3/4/5/5) preserve differentiation |

---

## Implementation Order

1. **Phase 1 first** (Fixes 1.1, 1.2, 1.3) — These make violations visible. Without them, the detector is blind.
2. **Phase 2 second** (Fixes 2.1, 2.3) — These prevent violations from happening. Fix 2.2 is skipped (keep 0.4).
3. **Phase 3 last** (Fixes 3.1, 3.2, 3.3) — These optimize throughput within safe bounds.

---

## Expected Outcome

| Metric | Before | After |
|--------|--------|-------|
| Max sustained rate | 10/sec (violates) | 5/sec (safe ceiling) |
| Handler FloodWait visibility | 0% | 100% |
| Flood pressure reaction time | Up to 10s | Immediate |
| Flood pressure compounding | 1.3× (capped by cooldown) | 1.15× per event (no cooldown) |
| Single-item batch size | 3 | 5 |
| Broadcast concurrency | 1 (sequential) | 2 (parallel) |
| Drain time (100 items, 1000 users) | ~2.3 hours | ~1.4 hours |
| Effective throughput (with dodge) | ~12,000/hr | ~18,000/hr (continuous) |
| Violation probability | High (adaptive modes) | Low (capped + proactive detection) |
| Recovery mode rate | 4/sec (40% of 10) | 2/sec (40% of 5) |