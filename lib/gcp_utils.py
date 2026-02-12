import subprocess
import time
import logging

def authenticate(key_file):
    """Authenticate using the service account key."""
    subprocess.run(["gcloud", "auth", "activate-service-account", "--key-file", key_file], 
                   check=True, capture_output=True)

def run_export(instance, database, target_uri, project_id):
    """Triggers export and handles both instant and long-running operations."""
    cmd = [
        "gcloud", "sql", "export", "sql", instance, target_uri,
        f"--database={database}", "--offload", "--quiet",
        f"--project={project_id}", "--format=value(name)", "--verbosity=error"
    ]
    
    start_time = time.time()
    try:
        # Capture the output from gcloud
        result = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode("utf-8").strip()
        
        # Take the last line to avoid pricing warnings or SDK debug noise
        last_line = result.splitlines()[-1]

        # CASE 1: Instant Success (Common for tiny databases)
        if "Exported" in last_line:
            logging.info(f"Instant export success for {database}.")
            return time.time() - start_time

        # CASE 2: Long-running Export (We have a real Operation ID)
        operation_id = last_line
        logging.info(f"Started polling for {database}. Op ID: {operation_id}")

        while True:
            check_cmd = [
                "gcloud", "sql", "operations", "describe", operation_id,
                f"--project={project_id}", "--format=value(status)"
            ]
            status = subprocess.check_output(check_cmd).decode("utf-8").strip()
            
            if status == "DONE":
                return time.time() - start_time
            elif status == "FAILED":
                raise Exception(f"GCP Operation {operation_id} failed on server side.")
            
            # Wait 30 seconds between checks
            time.sleep(30)
            
    except subprocess.CalledProcessError as e:
        error_msg = e.output.decode("utf-8") if e.output else str(e)
        raise Exception(f"Gcloud initiation failed: {error_msg}")