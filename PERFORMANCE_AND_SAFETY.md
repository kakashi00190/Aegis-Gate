# Bot Performance & Safety Documentation

This document outlines the performance improvements and safety measures implemented to ensure reliable operation and compliance with Telegram's Anti-Spam policies.

## 🚀 Performance Improvements

### **1. Optimized Broadcasting Speed**
- **Reduced Delay**: The base delay between individual messages has been reduced from `0.035s` to `0.02s` for manual broadcasts and optimized for concurrent processing in the background broadcast queue.
- **Concurrency**: The broadcast queue now handles up to **30 concurrent requests**, allowing for rapid delivery during high-load periods without overwhelming the system.
- **Batch Processing**: Database operations for sent messages are batched (20 IDs per batch) to minimize IO overhead.

### **2. Smooth Visual Feedback**
- **High-Resolution Progress Bars**: Implemented a sub-block character based progress bar (`░▏▎▍▌▋▊▉█`) that provides 8x smoother visual updates than traditional block-based bars.
- **Real-time Percentage Updates**: Progress updates are now sent on every 1% change, providing a more responsive UI for admins.

---

## 🛡️ Anti-Ban & Safety Measures

### **1. Global Rate Limiter (Token Bucket Algorithm)**
- **Strict Compliance**: A global `TokenBucketLimiter` is enforced across all message-sending components (Broadcast, Session Wipe, and Automated Notifications).
- **Rate Limit**: Hard-capped at **25 requests per second** (below Telegram's recommended 30 req/sec limit) to provide a safety margin.
- **Capacity**: Allows for a small burst of up to 30 messages before throttling kicks in.
- **Monitoring**: The limiter logs a warning every 50 waits, alerting administrators if the bot is hitting its traffic ceiling.

### **2. Adaptive Delay & Jitter**
- **Pattern Avoidance**: A random jitter (±20%) is applied to all message delays. This ensures the bot's request pattern is not perfectly mechanical, which helps avoid heuristic detection systems.
- **Dynamic Throttling**: If Telegram sends a `Retry-After` (429) error, the bot automatically pauses and resumes only after the specified cool-down period.

### **3. Smart User Status Management**
- **Block Detection**: The bot automatically marks users as `bot_blocked` in the database immediately upon receiving a `ForbiddenError`. This prevents the bot from repeatedly attempting to send messages to users who have blocked it, which is a major signal for spam detection.
- **Invalid ID Cleanup**: Similarly, accounts that are "Deactivated" or "Chat Not Found" are pruned from the notification list to maintain high delivery success rates.

### **4. Security Audits**
- **Admin Authentication**: All sensitive operations (Wipe, Broadcast, Config) are protected by a strict `is_admin` check against the `ADMIN_ID` configured in the environment.
- **Schema Validation**: All database queries have been audited to use current schema fields, avoiding the `broadcast_after` legacy column errors.

---

## 🏗️ Continuous Availability & Monitoring

### **1. Health Server**
- **Health Check Endpoint**: `/healthz` provides an automated health signal for monitoring tools.
- **WebSocket Stats**: A real-time stats endpoint (`/api/stats/ws`) allows for live monitoring of user growth and session activity without manual database queries.

### **2. Automated Failover Recommendations**
- The bot is designed to be stateless regarding its message queue (it's stored in Postgres), allowing it to recover instantly from restarts or container migrations.
