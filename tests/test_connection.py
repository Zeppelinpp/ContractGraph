import psycopg2
import subprocess
import os
from src.settings import settings

def connect_to_database(dbname: str):
    try:
        conn = psycopg2.connect(
            host=settings.pg_config["remote"]["host"],
            port=settings.pg_config["remote"]["port"],
            user=settings.pg_config["remote"]["user"],
            password=settings.pg_config["remote"]["password"],
            dbname=dbname
        )
        cur = conn.cursor()
        print(f"Connected to database: {dbname}")
        return conn, cur
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


def test_get_table_ddl(dbname: str, table_name: str):
    cmd = [
        "pg_dump",
        "-h", settings.pg_config["remote"]["host"],
        "-p", settings.pg_config["remote"]["port"],
        "-U", settings.pg_config["remote"]["user"],
        "-d", dbname,
        "-t", table_name,
        "-s",
    ]
    ddl = subprocess.check_output(cmd, text=True, env={**os.environ, "PGPASSWORD": settings.pg_config["remote"]["password"]})
    return ddl
if __name__ == "__main__":
    print(test_get_table_ddl("psdd_test_pg1_sys", "t_sec_user"))