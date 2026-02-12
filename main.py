import os
import datetime
import logging
import configparser
from concurrent.futures import ThreadPoolExecutor
from lib import gcp_utils, db_utils, cleanup_utils, notifier

# --- Configuration Paths (Server Side) ---
CONFIG_DIR = "/backup/configs"
KEY_FILE = "/root/jsonfiles/ti-dba-prod-01.json"
SSL_BASE_PATH = "/ssl-certs"
LOG_PATH = "/backup/logs"
MAX_WORKERS = 2  # SRE Safe-Start parallel instances

# Setup Logging
os.makedirs(LOG_PATH, exist_ok=True)
current_date = datetime.datetime.now().strftime("%Y-%m-%d")
logging.basicConfig(
    filename=f"{LOG_PATH}/MYSQL_backup_{current_date}.log",
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(threadName)s]: %(message)s'
)

def process_instance(instance_name, cfg, db_creds):
    """The task for each thread: Backup one instance's databases in order of size."""
    try:
        host = cfg[instance_name]['host']
        use_ssl = cfg[instance_name].get('ssl', 'n').lower() == 'y'
        ssl_path = os.path.join(SSL_BASE_PATH, instance_name)
        
        # 1. Get DBs sorted smallest-to-largest
        dbs = db_utils.get_databases_by_size(
            instance_name, host, 
            db_creds['DB_USR'], db_creds['DB_PWD'], 
            use_ssl, ssl_path
        )

        # 2. Sequential Export
        for db in dbs:
            target_uri = f"gs://ti-dba-prod-sql-01/Backups/Current/MYSQL/{instance_name}/{current_date}_{db}.sql.gz"
            duration = gcp_utils.run_export(instance_name, db, target_uri)
            logging.info(f"SUCCESS: {instance_name} | {db} | Duration: {duration:.2f}s")
            
    except Exception as e:
        logging.error(f"CRITICAL ERROR on Instance {instance_name}: {str(e)}")
        notifier.send_error(instance_name, str(e))

def main():
    logging.info("==== STARTING CLOUD SQL BACKUP PIPELINE ====")
    
    # Authenticate and Cleanup Housekeeping
    gcp_utils.authenticate(KEY_FILE)
    cleanup_utils.cleanup_logs(LOG_PATH, 30)

    # Load Server Configurations
    cfg = configparser.ConfigParser()
    cfg.read(os.path.join(CONFIG_DIR, "MYSQL_servers_list.conf"))
    
    creds = configparser.ConfigParser()
    creds.read(os.path.join(CONFIG_DIR, "db_credentials.conf"))
    db_creds = creds['credentials']

    # Parallel Execution
    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="SRE_Worker") as executor:
        for instance in cfg.sections():
            executor.submit(process_instance, instance, cfg, db_creds)

    logging.info("==== PIPELINE COMPLETED SUCCESSFULLY ====")

if __name__ == "__main__":
    main()