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
            # If a task hasn't reported in for 5 minutes, mark it as stalled
            # (Note: Some tasks might have longer sleep intervals, adjust accordingly)
            is_healthy = (now - last_seen) < 300
            status[name] = {
                "healthy": is_healthy,
                "last_seen": last_seen,
                "seconds_since_last_seen": round(now - last_seen, 1)
            }
        return status

health_monitor = TaskHealth()
