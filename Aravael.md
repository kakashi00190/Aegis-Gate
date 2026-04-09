# Aegis Gate — Telegram Media Sharing Bot

## Overview

A full-featured anonymous Telegram media sharing bot (@Aegis_Gatebot) built with Python, aiogram 3, and PostgreSQL. Published and running 24/7 via Aravael deployment.

## Stack

- **Language**: Python 3.11
- **Telegram framework**: aiogram 3.27.0
- **Database**: PostgreSQL + asyncpg (connection pool, min=3, max=20)
- **Monorepo tool**: pnpm workspaces (for TypeScript tooling)

## Bot Entry Point

All bot code lives in `bot/`:

```
bot/
├── main.py              # Entry point — starts bot, health server, background tasks
├── config.py            # Loads BOT_TOKEN, ADMIN_ID, DATABASE_URL from env
├── database.py          # All SQL queries + schema init (including pending_verifications table)
├── handlers/
│   ├── start.py         # /start command — DB-persisted verification, name stability
│   ├── media.py         # Media upload handler — atomic activation, broadcast on activate
│   ├── commands.py      # /me, /inspect, /leaderboard, /report, /help
│   └── admin.py         # Full admin panel (inline keyboard)
├── tasks/
│   ├── broadcast.py     # Queued media broadcast to active users
│   ├── inactivity.py    # Marks users inactive after timeout
│   ├── session.py       # Auto-ends session after duration
│   └── cleanup.py       # Deletes sent media from chats after session ends
└── utils/
    ├── helpers.py        # Link detection, badge helpers, time formatting
    ├── levels.py         # EXP → level calculation (fixed for level 1)
    ├── names.py          # Anonymous name generation
    └── session_announce.py # Session start/end announcements
```

## Required Secrets

- `BOT_TOKEN` — Telegram bot token from @BotFather
- `ADMIN_ID` — Telegram user ID of the admin
- `DATABASE_URL` — PostgreSQL connection URL (auto-provided by Aravael)
- `SESSION_SECRET` — Available but unused by Python bot

## Deployment

**Production**: `artifacts/bot-app` artifact — runs `python3 bot/main.py` with `PORT=20302`.
Health check at `/healthz`. This is the ONLY instance that should poll Telegram.

**Dev workflow**: "Telegram Bot" workflow is intentionally a NO-OP (prints a warning + sleeps).
Do NOT change it back to `python3 main.py` while the production deployment is live — running two
instances with the same BOT_TOKEN causes TelegramConflictError and split/duplicate responses.

## Bug Fixes Applied

1. **Progress bar** — Fixed `exp_progress()` for Level 1 (was showing -100/300, now shows 0/300)
2. **Activation spam** — `activate_user()` is now atomic (`WHERE status='pending'`) — prevents race conditions
3. **Reactivation spam** — `reactivate_user()` is now atomic (`WHERE status='inactive'`)
4. **Pause message spam** — Per-user 60-second cooldown on "uploads paused" message
5. **Name stability** — Verification state stored in `pending_verifications` DB table (survives restarts)
6. **Activation broadcast** — Media that triggers activation is now queued for broadcast
7. **Reactivation broadcast** — Media that triggers reactivation is now queued for broadcast
8. **Start cooldown** — /start has 5-second per-user cooldown to prevent duplicate responses
9. **Dual-instance conflict** — Dev "Telegram Bot" workflow disabled; only production bot polls
10. **Broadcast performance** — Reduced batch size (50→5), chunked recipients (25 per batch), added per-send delay, lowered concurrency (8→5) to prevent DB pool exhaustion and Telegram rate limiting
11. **DB pool exhaustion** — Increased pool max from 10→20 to prevent handler starvation during broadcasts
12. **Security dependency updates** — lodash 4.18.0, path-to-regexp 8.4.0, picomatch 2.3.2/4.0.4, aiohttp ≥3.13.4, vite ≥7.3.2
13. **Advanced broadcast system** — Exponential backoff with up to 3 retries, per-broadcast progress logging (sent/failed/elapsed), adaptive rate-limit handling
14. **Crash-safe cleanup** — Session message cleanup now uses batch processing (200 at a time), concurrent deletes (8 workers), incremental DB cleanup (processed messages removed immediately, not at the end). Survives bot restarts mid-cleanup.
15. **Leaderboard reset bug** — `end_session()` now runs in a proper DB transaction so badge awards + media delete + leaderboard reset are atomic (all-or-nothing). `create_new_session()` also resets `session_upload_count` as a safety net.
16. **Reactivation threshold too harsh** — Added separate `reactivation_threshold` (default: 3) so inactive users don't need 10 uploads to reactivate
17. **No upload progress feedback** — Pending and inactive users now see progress (e.g., "3/10 — Upload 7 more to activate")
18. **Level-up notifications missing** — Pending and inactive users now get level-up notifications on upload
19. **Stale verifications never cleaned** — Added hourly cleanup of pending verifications older than 24h
20. **Admin stats missing unverified count** — Stats now show "Unverified" count (users who started /start but never completed captcha)
21. **Session announcements wrong threshold** — Inactive users were told to upload 10 files to reactivate; now shows correct reactivation threshold
22. **Memory leak in cooldown dicts** — Added TTL-based cleanup to prevent unbounded memory growth
23. **Deprecated asyncio.get_event_loop()** — Replaced with asyncio.get_running_loop() to fix deprecation warnings

## Bot Features

- Anonymous registration with math verification (anti-bot)
- Unique permanent anonymous names (stable across restarts)
- EXP/level system: earn 10 EXP per upload, level up at EXP thresholds
- Session system (7-day sessions) with pause between sessions
- Advanced media broadcast: uploads from active users sent to all other active users, with exponential backoff, chunked delivery, rate-limit awareness, and per-broadcast progress logging
- Activation threshold (default: 10 uploads) to become active
- Reactivation threshold (default: 3 uploads) to reactivate inactive users
- Upload progress feedback for pending and inactive users
- Inactivity tracking: users inactive >160 min are marked inactive
- Automatic cleanup of stale pending verifications (24h TTL)
- Leaderboard (session-based upload count ranking, reset atomically at session end)
- Permanent badges for top uploaders at session end
- Report system: users can report media, admin reviews via inline keyboard
- Admin panel: ban/unban, config, stats, broadcast, session management
- Session end cleanup: concurrent batch deletion of all broadcast messages from user chats (crash-safe, incremental progress)
