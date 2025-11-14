import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    @property
    def pg_config(self):
        return {
            "host": os.getenv("PG_HOST"),
            "port": os.getenv("PG_PORT"),
            "user": os.getenv("PG_USER"),
            "password": os.getenv("PG_PASSWORD"),
        }


settings = Settings()