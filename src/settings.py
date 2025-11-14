import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    @property
    def pg_config(self):
        db_config = {
            "remote": {
                "host": os.getenv("PG_REMOTE_HOST"),
                "port": os.getenv("PG_REMOTE_PORT"),
                "user": os.getenv("PG_REMOTE_USER"),
                "password": os.getenv("PG_REMOTE_PASSWORD"),
            },
            "local": {
                "host": os.getenv("PG_LOCAL_HOST"),
                "port": os.getenv("PG_LOCAL_PORT"),
                "user": os.getenv("PG_LOCAL_USER"),
                "password": os.getenv("PG_LOCAL_PASSWORD"),
            },
        }
        return db_config

    @property
    def nebula_config(self):
        return {
            "host": os.getenv("NEBULA_HOST"),
            "port": os.getenv("NEBULA_PORT"),
            "user": os.getenv("NEBULA_USERNAME"),
            "password": os.getenv("NEBULA_PASSWORD"),
            "space": os.getenv("NEBULA_SPACE"),
        }


settings = Settings()
