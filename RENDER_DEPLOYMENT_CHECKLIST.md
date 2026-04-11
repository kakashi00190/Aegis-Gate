# Render Deployment Checklist (Media-Bot-Share)

Follow these steps to ensure a smooth deployment to Render:

## 1. Environment Variables
Ensure the following environment variables are correctly set in the Render dashboard:
- [ ] `BOT_TOKEN`: Your Telegram Bot API token.
- [ ] `ADMIN_ID`: Your Telegram user ID (or a comma-separated list for multiple admins).
- [ ] `DATABASE_URL`: The PostgreSQL connection string (Render's internal URL is preferred).
- [ ] `PORT`: Set to `8080` (or another port of your choice).

## 2. Startup Command
Use the following command to start the bot:
```bash
python bot/validate_env.py && python bot/main.py
```
This ensures the bot only starts if all environment variables are correct and the database is reachable.

## 3. Health Checks
Configure Render's health check to monitor the following endpoint:
- **Path**: `/api/healthz`
- **Port**: `8080` (or whatever you set in `PORT`)
- **Protocol**: `HTTP`

*Note: For detailed task monitoring, use `/api/health`.*

## 4. Database Optimization
- [ ] Ensure you are using at least a **Starter** (non-free) PostgreSQL instance if you have more than 500 users. The free tier has strict connection limits (max 50) and may cause issues during high-volume broadcasts.
- [ ] Set `asyncpg` pool `max_size` to `50` in `bot/main.py` (already configured, but verify if you change database tiers).

## 5. Post-Deployment Verification
Once deployed, verify the following:
- [ ] **Admin Panel**: Send `/admin` to the bot to ensure it responds and shows the admin panel.
- [ ] **Stats**: Check the "Stats" button in the admin panel to ensure the database connection is active and data is being fetched.
- [ ] **Broadcast Queue**: Upload a test media file. It should be claimed and sent to other active users after the `broadcast_delay_seconds` (default: 30s).
- [ ] **Health API**: Visit `https://your-app-name.onrender.com/api/health` to confirm all background tasks are reporting "healthy".

## 6. Rollback Plan
If the bot fails:
1. Check the Render logs for "Environment validation failed" or "Session check error".
2. If the failure is code-related, use Render's "Rollback to previous commit" feature to revert to the last known stable commit (`820f78f`).
