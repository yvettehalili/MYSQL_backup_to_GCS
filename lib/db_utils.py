import subprocess
import logging

def get_databases_by_size(instance_name, host, user, password, use_ssl, ssl_path):
    """Queries information_schema to sort databases by size ascending."""
    query = (
        "SELECT table_schema, SUM(data_length + index_length) / 1024 / 1024 AS size_mb "
        "FROM information_schema.TABLES "
        "WHERE table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys') "
        "GROUP BY table_schema "
        "ORDER BY size_mb ASC;"
    )

    cmd = [
        "mysql", f"-u{user}", f"-p{password}", f"-h{host}",
        "-B", "--silent", "-N", "-e", query
    ]

    if use_ssl:
        cmd += [
            f"--ssl-ca={ssl_path}/server-ca.pem",
            f"--ssl-cert={ssl_path}/client-cert.pem",
            f"--ssl-key={ssl_path}/client-key.pem"
        ]

    try:
        result = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode("utf-8")
        # Extract just the database names from the tab-separated result
        db_list = [line.split('\t')[0] for line in result.strip().split('\n') if line]
        return db_list
    except Exception as e:
        logging.error(f"Metadata query failed for {instance_name}, falling back to gcloud list: {e}")
        return fallback_list(instance_name)

def fallback_list(instance):
    """Standard gcloud list if MySQL connection fails."""
    cmd = ["gcloud", "sql", "databases", "list", f"--instance={instance}", "--format=value(name)"]
    res = subprocess.check_output(cmd).decode("utf-8").strip().split('\n')
    exclude = ("information_schema", "performance_schema", "sys", "mysql")
    return [db for db in res if db not in exclude]