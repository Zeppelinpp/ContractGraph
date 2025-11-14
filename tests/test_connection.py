import psycopg2
from src.settings import settings

def connect_to_database(dbname: str):
    try:
        conn = psycopg2.connect(
            host=settings.pg_config["host"],
            port=settings.pg_config["port"],
            user=settings.pg_config["user"],
            password=settings.pg_config["password"],
            dbname=dbname
        )
        cur = conn.cursor()
        print(f"Connected to database: {dbname}")
        return conn, cur
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


if __name__ == "__main__":
    conn, cur = connect_to_database("psdd_test_pg1_sys")
    print(conn)
    print(cur)