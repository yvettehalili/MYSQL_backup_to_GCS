import os
import datetime
import logging
import configparser
from concurrent.futures import ThreadPoolExecutor
from lib import gcp_utils, db_utils, cleanup_utils, notifier

# --- CONFIGURATION ---
MAX_WORKERS = 2 
CONFIG_DIR = "/backup/configs"
KEY_FILE = "/root/jsonfiles/ti-dba-prod-01.json"
BUCKET = "gs://ti-dba-bucket"
GCS_PATH = "Backups/Current/MYSQL"
LOG_DIR = "/backup/logs"

# Setup logging with thread names to track parallel execution
current_date = datetime.datetime.now().strftime("%Y-%m-%d")
logging.basicConfig(
    filename=f"{LOG_DIR}/MYSQL_backup_{current_date}.log",
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(threadName)s]: %(message)s'
)

def process_instance(instance_name, config, db_creds):
    """Function executed by each thread for a specific Cloud SQL instance."""
    try:
        host = config[instance_name]['host']
        use_ssl = config[instance_name].get('ssl', 'n').lower() == 'y'
        ssl_path = f"/ssl-certs/{instance_name}"
        
        # 1. Discover Databases and sort by Smallest First
        dbs = db_utils.get_databases_by_size(
            instance_name, host, 
            db_creds['DB_USR'], db_creds['DB_PWD'], 
            use_ssl, ssl_path
        )
        
        # 2. Sequential Export for databases within THIS instance
        for db in dbs:
            try:
                logging.info(f"STARTING EXPORT: {instance_name} | DB: {db}")
                duration = gcp_utils.run_export(instance_name, db, BUCKET, GCS_PATH, current_date)
                logging.info(f"SUCCESS: {instance_name} | {db} | Duration: {duration:.2f}s")
            except Exception as e:
                logging.error(f"DATABASE ERROR: {instance_name} | {db} | {e}")
                
    except Exception as e:
        logging.error(f"INSTANCE CRITICAL ERROR: {instance_name} | {e}")
        notifier.send_error(instance_name, str(e))

def main():
    logging.info("==== MYSQL BACKUP PIPELINE STARTING ====")
    
    # Housekeeping: Cleanup old logs and Authenticate GCP
    cleanup_utils.cleanup_logs(LOG_DIR, 30)
    gcp_utils.authenticate(KEY_FILE)

    # Load Infrastructure Config and DB Credentials
    cfg = configparser.ConfigParser()
    cfg.read(f"{CONFIG_DIR}/MYSQL_servers_list.conf")
    
    creds = configparser.ConfigParser()
    creds.read(f"{CONFIG_DIR}/db_credentials.conf")
    db_creds = creds['credentials']

    # Parallel Execution Logic
    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="SRE_Worker") as executor:
        for instance in cfg.sections():
            executor.submit(process_instance, instance, cfg, db_creds)

    logging.info("==== MYSQL BACKUP PIPELINE FINISHED ====")

if __name__ == "__main__":
    main()