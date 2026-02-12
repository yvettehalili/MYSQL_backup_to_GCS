import os
import datetime
import logging
import configparser
from concurrent.futures import ThreadPoolExecutor
from lib import gcp_utils, db_utils, cleanup_utils, notifier

# --- Config Paths ---
CONFIG_DIR = "/backup/configs"
KEY_FILE = "/root/jsonfiles/ti-dba-prod-01.json"
SSL_BASE_PATH = "/ssl-certs"
LOG_PATH = "/backup/logs"
MAX_WORKERS = 2 

# Setup Logging
os.makedirs(LOG_PATH, exist_ok=True)
current_date = datetime.datetime.now().strftime("%Y-%m-%d")
logging.basicConfig(
    filename=f"{LOG_PATH}/MYSQL_backup_{current_date}.log",
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(threadName)s]: %(message)s'
)

def process_instance(instance_name, cfg, db_creds):
    """Processes databases for an instance, continuing on individual failures."""
    try:
        host = cfg[instance_name]['host']
        project_id = cfg[instance_name].get('project', 'ti-dba-prod-01')
        bucket = cfg[instance_name].get('bucket', 'ti-dba-bucket')
        base_path = cfg[instance_name].get('base_path', 'Backups/Current/MYSQL')
        use_ssl = cfg[instance_name].get('ssl', 'n').lower() == 'y'
        ssl_path = os.path.join(SSL_BASE_PATH, instance_name)
        
        # Get list of databases
        dbs = db_utils.get_databases_by_size(
            instance_name, host, 
            db_creds['DB_USR'], db_creds['DB_PWD'], 
            use_ssl, ssl_path
        )

        for db in dbs:
            try:
                target_uri = f"gs://{bucket}/{base_path}/{instance_name}/{current_date}_{db}.sql.gz"
                logging.info(f"Initiating: {instance_name} -> {db}")
                
                # Execute export
                duration = gcp_utils.run_export(instance_name, db, target_uri, project_id)
                logging.info(f"SUCCESS: {instance_name} | {db} | Time: {duration:.2f}s")
                
            except Exception as e:
                # CRITICAL: This allows the loop to move to the next DB if one fails
                logging.error(f"DATABASE FAILED: {db} on {instance_name}. Error: {str(e)}")
                notifier.send_error(f"{instance_name} - {db}", str(e))
                continue 

    except Exception as e:
        logging.error(f"INSTANCE FATAL ERROR {instance_name}: {str(e)}")
        notifier.send_error(instance_name, f"Fatal error fetching DB list: {str(e)}")

def main():
    logging.info("==== STARTING BACKUP PIPELINE (ROBUST MODE) ====")
    gcp_utils.authenticate(KEY_FILE)
    cleanup_utils.cleanup_logs(LOG_PATH, 30)

    cfg = configparser.ConfigParser()
    cfg.read(os.path.join(CONFIG_DIR, "MYSQL_servers_list.conf"))
    
    creds = configparser.ConfigParser()
    creds.read(os.path.join(CONFIG_DIR, "db_credentials.conf"))
    db_creds = creds['credentials']

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for instance in cfg.sections():
            executor.submit(process_instance, instance, cfg, db_creds)

if __name__ == "__main__":
    main()