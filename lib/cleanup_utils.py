import os
import time

def cleanup_logs(log_path, days_to_keep):
    now = time.time()
    cutoff = now - (days_to_keep * 86400)
    for f in os.listdir(log_path):
        f_path = os.path.join(log_path, f)
        if os.path.isfile(f_path) and os.path.getmtime(f_path) < cutoff:
            os.remove(f_path)