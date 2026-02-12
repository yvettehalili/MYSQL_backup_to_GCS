import subprocess

def send_error(instance, error_msg):
    # This calls your existing error notification script
    cmd = ["python3", "/backup/scripts/MYSQL_backup_error_notif.py", instance, error_msg]
    subprocess.run(cmd)