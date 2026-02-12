import subprocess
import time
import logging

def authenticate(key_file):
    """Authenticate using the service account key."""
    subprocess.run(["gcloud", "auth", "activate-service-account", "--key-file", key_file], 
                   check=True, capture_output=True)

def run_export(instance, database, target_uri, project_id):
    """Triggers export with --async and polls until DONE to avoid 409 conflicts."""
    # --async makes gcloud return the Operation ID immediately
    cmd = [
        "gcloud", "sql", "export", "sql", instance, target_uri,
        f"--database={database}", "--offload", "--quiet", "--async",
        f"--project={project_id}", "--format=value(name)", "--verbosity=error"
    ]
    
    start_time = time.time()
    operation_id = None

    # Attempt to trigger the export; retry if instance is temporarily busy (409)
    for attempt in range(3):
        try:
            result = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode("utf-8").strip()
            # With --async and formatting, the last line is reliably the ID
            operation_id = result.splitlines()[-1]
            break
        except subprocess.CalledProcessError as e:
            error_msg = e.output.decode("utf-8")
            # If another operation is running, wait 60s and try again
            if "409" in error_msg and attempt < 2:
                logging.warning(f"Instance {instance} busy, retrying {database} in 60s... (Attempt {attempt+1}/3)")
                time.sleep(60)
            else:
                raise Exception(f"Gcloud initiation failed: {error_msg}")

    if not operation_id:
        raise Exception(f"Failed to obtain Operation ID for {database}")

    logging.info(f"Export triggered for {database}. Polling Op ID: {operation_id}")

    # Polling loop: This thread waits here so the next DB doesn't start too early
    while True:
        check_cmd = [
            "gcloud", "sql", "operations", "describe", operation_id,
            f"--project={project_id}", "--format=value(status)"
        ]
        try:
            status = subprocess.check_output(check_cmd).decode("utf-8").strip()
            
            if status == "DONE":
                return time.time() - start_time
            elif status == "FAILED":
                raise Exception(f"GCP Operation {operation_id} failed on server side.")
        except subprocess.CalledProcessError:
            # Occasionally describe fails if the operation is too new; just retry
            pass
        
        time.sleep(30)