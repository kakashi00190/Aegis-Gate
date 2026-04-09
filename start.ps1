# Production Startup Script for Media Sharing Bot

# Set working directory to the script's location
Set-Location -Path $PSScriptRoot

$TELEGRAM_BOT_TOKEN = "7359246692:AAG_6q1HopvyuZiAsUyWV93NU-i9cSDgKF8"
$ADMIN_USER_IDS = "8557088722"
$PORT = "3000"
$DATABASE_URL = "postgresql://postgres.qxaicegctimexcyxdngk:%40Dev_Raj48124812@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres"

# Set environment variables (for both original config and user prompt names)
$env:TELEGRAM_BOT_TOKEN = $TELEGRAM_BOT_TOKEN
$env:BOT_TOKEN = $TELEGRAM_BOT_TOKEN
$env:ADMIN_USER_IDS = $ADMIN_USER_IDS
$env:ADMIN_ID = $ADMIN_USER_IDS
$env:PORT = $PORT
$env:DATABASE_URL = $DATABASE_URL

Write-Host "--- Environment Configured ---" -ForegroundColor Cyan
Write-Host "BOT_TOKEN: [REDACTED]"
Write-Host "ADMIN_ID: $env:ADMIN_ID"
Write-Host "PORT: $env:PORT"
Write-Host "DATABASE_URL: [REDACTED]"

# Validate Environment
Write-Host "`n--- Running Validation Checks ---" -ForegroundColor Yellow
python bot/validate_env.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "`n❌ Validation failed. Aborting startup." -ForegroundColor Red
    exit 1
}

# If validation passes, start the bot
Write-Host "`n--- Validation Successful. Starting Bot ---" -ForegroundColor Green
python bot/main.py
