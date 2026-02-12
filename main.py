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
    """Sequential export of databases for a specific instance."""
    try:
        host = cfg[instance_name]['host']
        # Project ID discovery (defaults to prod if not specified)
        project_id = cfg[instance_name].get('project', 'ti-dba-prod-01')
        use_ssl = cfg[instance_name].get('ssl', 'n').lower() == 'y'
        ssl_path = os.path.join(SSL_BASE_PATH, instance_name)
        
        # 1. Fetch databases sorted by size (Smallest first)
        dbs = db_utils.get_databases_by_size(
            instance_name, host, 
            db_creds['DB_USR'], db_creds['DB_PWD'], 
            use_ssl, ssl_path
        )

        # 2. Loop through databases (Wait for each to finish before next)
        for db in dbs:
            target_uri = f"gs://ti-dba-prod-sql-01/Backups/Current/MYSQL/{instance_name}/{current_date}_{db}.sql.gz"
            logging.info(f"Initiating: {instance_name} -> {db}")
            
            duration = gcp_utils.run_export(instance_name, db, target_uri, project_id)
            
            logging.info(f"COMPLETED: {instance_name} | {db} | Time: {duration:.2f}s")
            
    except Exception as e:
        logging.error(f"CRITICAL ERROR on {instance_name}: {str(e)}")
        notifier.send_error(instance_name, str(e))

def main():
    logging.info("==== STARTING BACKUP PIPELINE (POLLING MODE) ====")
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