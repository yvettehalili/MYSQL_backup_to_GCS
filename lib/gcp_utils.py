import subprocess
import time
import logging

def authenticate(key_file):
    """Authenticate using the service account key."""
    subprocess.run(["gcloud", "auth", "activate-service-account", "--key-file", key_file], 
                   check=True, capture_output=True)

def run_export(instance, database, target_uri, project_id):
    """Triggers export and polls accurately by filtering for the Operation ID."""
    cmd = [
        "gcloud", "sql", "export", "sql", instance, target_uri,
        f"--database={database}", "--offload", "--quiet",
        f"--project={project_id}", "--format=value(name)"
    ]
    
    start_time = time.time()
    try:
        # We use stderr=subprocess.STDOUT to merge warnings so we can handle them
        result = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode("utf-8").strip()
        
        # Get the LAST line of the output (which will be the ID, skipping pricing warnings)
        operation_id = result.splitlines()[-1] 
        
        if not operation_id:
            raise Exception("Could not retrieve Operation ID from gcloud output.")

        logging.info(f"Started export for {database}. Op ID: {operation_id}")

        while True:
            check_cmd = [
                "gcloud", "sql", "operations", "describe", operation_id,
                f"--project={project_id}", "--format=value(status)"
            ]
            status = subprocess.check_output(check_cmd).decode("utf-8").strip()
            
            if status == "DONE":
                return time.time() - start_time
            elif status == "FAILED":
                raise Exception(f"GCP Operation {operation_id} failed on the server side.")
            
            time.sleep(30)
            
    except subprocess.CalledProcessError as e:
        # Extract the actual error message from the failed command
        error_output = e.output.decode("utf-8") if e.output else str(e)
        logging.error(f"Failed to initiate export: {error_output}")
        raise