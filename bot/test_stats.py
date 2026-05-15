import asyncio
import os
import asyncpg
import logging
import sys

# Add bot directory to sys.path
sys.path.append(os.path.join(os.getcwd(), 'bot'))

from database import get_advanced_stats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stats_test")

async def test_stats_accuracy():
    # Force use of correct python executable environment
    import os
    import sys
    
    # Map the correct DATABASE_URL if not set
    if not os.environ.get("DATABASE_URL"):
        os.environ["DATABASE_URL"] = "postgresql://postgres.qxaicegctimexcyxdngk:%40Dev_Raj48124812@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres"

    db_url = os.environ.get("DATABASE_URL")

    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    try:
        pool = await asyncpg.create_pool(db_url, statement_cache_size=0)
        logger.info("✅ Connected to database.")

        stats = await get_advanced_stats(pool)
        
        logger.info("Current Statistics:")
        for key, value in stats.items():
            if key not in ['session', 'top3']:
                logger.info(f"  - {key}: {value}")

        # Validation Rule: Active + Inactive + Pending + Banned + Blocked bot MUST equal total_users
        calculated_total = (
            stats['active'] + 
            stats['inactive'] + 
            stats['pending'] + 
            stats['banned'] + 
            stats['blocked_bot']
        )
        
        actual_total = stats['total_users']
        
        logger.info(f"Validation Check: {calculated_total} (Calculated) vs {actual_total} (Actual Total Users)")
        
        if calculated_total == actual_total:
            logger.info("✅ SUCCESS: Validation rule passed!")
        else:
            logger.error("❌ FAILURE: Statistics inconsistency detected!")
            
        # Also check total including unverified
        total_all = actual_total + stats['unverified']
        if total_all == stats['total']:
            logger.info(f"✅ SUCCESS: Grand Total validation passed! ({total_all})")
        else:
            logger.error(f"❌ FAILURE: Grand Total inconsistency! {total_all} vs {stats['total']}")

        await pool.close()
    except Exception as e:
        logger.error(f"❌ Error during test: {e}")

if __name__ == "__main__":
    asyncio.run(test_stats_accuracy())
