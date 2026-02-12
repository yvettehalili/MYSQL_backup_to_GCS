import subprocess
import time

def authenticate(key_file):
    subprocess.run(["gcloud", "auth", "activate-service-account", "--key-file", key_file], 
                   check=True, capture_output=True)

def run_export(instance, database, target_uri):
    cmd = ["gcloud", "sql", "export", "sql", instance, target_uri, f"--database={database}", "--offload", "--quiet"]
    start = time.time()
    subprocess.run(cmd, check=True, capture_output=True)
    return time.time() - start