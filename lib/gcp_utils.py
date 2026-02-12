import subprocess
import logging
import time

def authenticate(key_file):
    """Activate GCP Service Account."""
    try:
        subprocess.run(["gcloud", "auth", "activate-service-account", "--key-file", key_file], 
                       check=True, capture_output=True)
        logging.info("GCP Authentication Successful.")
    except subprocess.CalledProcessError as e:
        raise Exception(f"GCP Auth Failed: {e.stderr.decode()}")

def run_export(instance, database, bucket, gcs_path, current_date):
    """Triggers GCP server-side export with --offload."""
    target_uri = f"{bucket}/{gcs_path}/{instance}/{current_date}_{database}.sql.gz"
    cmd = [
        "gcloud", "sql", "export", "sql", instance, target_uri,
        f"--database={database}", "--offload", "--quiet"
    ]
    start_time = time.time()
    # check=True raises an exception if the gcloud command fails
    subprocess.run(cmd, check=True, capture_output=True)
    return time.time() - start_time