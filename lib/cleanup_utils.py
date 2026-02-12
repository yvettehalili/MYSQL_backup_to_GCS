import os
import time
import logging

def cleanup_logs(log_path, days_to_keep=30):
    """Rotates logs to prevent local disk exhaustion."""
    now = time.time()
    cutoff = now - (days_to_keep * 86400)
    count = 0
    try:
        if not os.path.exists(log_path):
            return
        for f in os.listdir(log_path):
            file_path = os.path.join(log_path, f)
            if os.path.isfile(file_path) and os.path.getmtime(file_path) < cutoff:
                os.remove(file_path)
                count += 1
        if count > 0:
            logging.info(f"Cleanup: Deleted {count} log files older than {days_to_keep} days.")
    except Exception as e:
        logging.error(f"Cleanup failed: {e}")