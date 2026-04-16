import time
import logging

logger = logging.getLogger(__name__)

class TaskHealth:
    _tasks = {}

    @classmethod
    def update(cls, task_name: str):
        cls._tasks[task_name] = time.time()

    @classmethod
    def get_status(cls) -> dict:
        now = time.time()
        status = {}
        for name, last_seen in cls._tasks.items():
            # Increase threshold to 1 hour to accommodate long-running tasks and sleep intervals
            # (e.g. check_session_end sleeps for 30 mins)
            is_healthy = (now - last_seen) < 3600
            status[name] = {
                "healthy": is_healthy,
                "last_seen": last_seen,
                "seconds_since_last_seen": round(now - last_seen, 1)
            }
        return status

health_monitor = TaskHealth()
