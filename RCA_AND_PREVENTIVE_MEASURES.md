# Root Cause Analysis (RCA) & Preventive Measures

## Incident Summary
The bot experienced two major failures since the last deployment:
1.  **Media sharing stopped entirely**: Users were stuck with "Uploads are paused" even after the pause period should have ended.
2.  **Severe performance degradation**: Bot response times exceeded 17 seconds, leading to timeout errors and missed updates.

## Root Cause Analysis

### 1. Session Task Crash (Stuck in Pause)
-   **Cause**: An `AttributeError` occurred in the `check_session_end` background task. The code attempted to use `.get()` on a database record object (from `asyncpg`), which does not support the `.get()` method (it uses dictionary-style or attribute-style access).
-   **Impact**: The background task that monitors session transitions crashed. Consequently, the logic to end the "pause" state and start a new session never ran.
-   **File affected**: `bot/tasks/session.py`

### 2. API Rate Limiting (Performance Lag)
-   **Cause**: The `delete_session_messages` (cleanup) task was attempting to send real-time progress updates (progress bars) to **every single user** in the database.
-   **Impact**: With a growing user base, this generated thousands of Telegram API calls (`edit_text`) in a very short window. Telegram's rate limiter triggered, forcing the bot to wait for extended periods. This blocked the `aiogram` event loop, causing massive delays (17s+) for all other updates.
-   **File affected**: `bot/tasks/cleanup.py`

---

## Preventive Measures Implemented

### 1. Code Fixes & Optimizations
-   **Corrected Record Access**: Fixed all instances of `.get()` on database records to use standard index-based access (`['field']`).
-   **Progress Bar Optimization**: The cleanup task now only sends real-time progress bars to the **Admin**. Regular users receive a single "Wiping media" notification at the start and a "Wipe complete" notification at the end. This reduces API load by 99% during cleanup.
-   **Reduced Polling Frequency**: Increased the stats broadcast interval from 5s to 15s to reduce database and network overhead.

### 2. Robust Error Monitoring
-   **Health Monitor**: Implemented a `TaskHealth` utility that tracks the heartbeat of all critical background tasks (broadcast, session, inactivity, cleanup).
-   **Detailed Health Endpoint**: Added `/api/health` which returns the status of every background task. This allows for proactive detection of stalled or crashed tasks before they impact users.
-   **Startup Validation**: Enhanced `validate_env.py` to check for database connectivity, schema existence, and environment variable validity before the bot even starts.

### 3. Stability Protocols
-   **Safe Deployment Check**: Added a startup check that prevents the bot from starting if critical dependencies or environment variables are missing.
-   **Broadcast Safeguards**: Added a "Check recipients BEFORE claiming" logic in the broadcast queue to prevent marking media as sent when there are no active users to receive it.

## Future Recommendations
-   **Sentry Integration**: Consider adding Sentry for real-time error tracking and alerting.
-   **Database Indexing**: Regularly monitor slow queries and ensure `scheduled_at` and `sent_at` are indexed (already implemented, but should be monitored).
