import psycopg2
import subprocess
import os
from src.settings import settings
from nebula3.gclient.net import ConnectionPool, Session
from nebula3.Config import Config

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


def test_connect_nebula():
    config = Config()
    config.max_connection_pool_size = 10
    
    connection_pool = ConnectionPool()
    ok = connection_pool.init([(settings.nebula_config["host"], settings.nebula_config["port"])], config)
    if not ok:
        raise Exception("Failed to initialize connection pool")
    
    session = connection_pool.get_session(settings.nebula_config["user"], settings.nebula_config["password"])
    session.execute(f"USE {settings.nebula_config['space']}")


    
if __name__ == "__main__":
    # print(test_get_table_ddl("psdd_test_pg1_sys", "t_sec_user"))
    test_connect_nebula()