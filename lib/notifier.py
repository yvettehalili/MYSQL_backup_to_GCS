import subprocess
import logging

def send_error(instance, error_message):
    """Calls your external notification script to send email alerts."""
    try:
        # Example of calling your existing MYSQL_backup_error_notif.py
        # Adjust the path to where your actual email script resides
        cmd = ["python3", "/backup/scripts/MYSQL_backup_error_notif.py", instance, error_message]
        subprocess.run(cmd, check=True)
    except Exception as e:
        logging.error(f"Failed to send email notification: {e}")