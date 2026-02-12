import subprocess
import time
import logging

def authenticate(key_file):
    """Authenticate using the service account key."""
    subprocess.run(["gcloud", "auth", "activate-service-account", "--key-file", key_file], 
                   check=True, capture_output=True)

def run_export(instance, database, target_uri, project_id):
    """Triggers export and polls the operation until completion."""
    # We use --format=value(name) to get the Operation ID string
    cmd = [
        "gcloud", "sql", "export", "sql", instance, target_uri,
        f"--database={database}", "--offload", "--quiet",
        f"--project={project_id}", "--format=value(name)"
    ]
    
    start_time = time.time()
    try:
        # 1. Start the Export (Async)
        operation_id = subprocess.check_output(cmd).decode("utf-8").strip()
        logging.info(f"Started export for {database}. Op ID: {operation_id}")

        # 2. Polling Loop
        while True:
            check_cmd = [
                "gcloud", "sql", "operations", "describe", operation_id,
                f"--project={project_id}", "--format=value(status)"
            ]
            status = subprocess.check_output(check_cmd).decode("utf-8").strip()
            
            if status == "DONE":
                duration = time.time() - start_time
                return duration
            elif status == "FAILED":
                raise Exception(f"GCP Operation {operation_id} failed on the server side.")
            
            # Wait 30 seconds before checking again to be kind to the API
            time.sleep(30)
            
    except subprocess.CalledProcessError as e:
        error_msg = e.output.decode("utf-8") if e.output else str(e)
        logging.error(f"Failed to initiate export for {database}: {error_msg}")
        raise