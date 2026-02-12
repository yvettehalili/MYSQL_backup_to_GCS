import subprocess
import logging

def get_databases_by_size(instance_name, host, user, password, use_ssl, ssl_path):
    """Queries metadata and cleans output to avoid 'Warning' messages."""
    """Queries metadata and cleans output to avoid 'Warning' messages."""
    query = (
        "SELECT table_schema, SUM(data_length + index_length) / 1024 / 1024 AS size_mb "
        "FROM information_schema.TABLES "
        "WHERE table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys') "
        "GROUP BY table_schema "
        "ORDER BY size_mb ASC;"
    )

    # We use -p with no space to minimize some warning triggers, 
    # but we must handle the text output carefully.
    # We use -p with no space to minimize some warning triggers, 
    # but we must handle the text output carefully.
    cmd = [
        "mysql", f"-u{user}", f"-p{password}", f"-h{host}",
        "-B", "--silent", "-N", "-e", query
    ]

    if use_ssl:
        cmd += [f"--ssl-ca={ssl_path}/server-ca.pem", f"--ssl-cert={ssl_path}/client-cert.pem", f"--ssl-key={ssl_path}/client-key.pem"]
        cmd += [f"--ssl-ca={ssl_path}/server-ca.pem", f"--ssl-cert={ssl_path}/client-cert.pem", f"--ssl-key={ssl_path}/client-key.pem"]

    try:
        # Capture the output
        # Capture the output
        result = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode("utf-8")
        
        db_list = []
        for line in result.strip().split('\n'):
            line = line.strip()
            # SRE FIX: Skip lines that are warnings or empty
            if not line or "Warning" in line or "password on the command line" in line:
                continue
            
            # Extract the first column (database name)
            db_name = line.split('\t')[0]
            db_list.append(db_name)
            
        
        db_list = []
        for line in result.strip().split('\n'):
            line = line.strip()
            # SRE FIX: Skip lines that are warnings or empty
            if not line or "Warning" in line or "password on the command line" in line:
                continue
            
            # Extract the first column (database name)
            db_name = line.split('\t')[0]
            db_list.append(db_name)
            
        return db_list
    except Exception as e:
        logging.error(f"Metadata query failed for {instance_name}: {e}")
        return []