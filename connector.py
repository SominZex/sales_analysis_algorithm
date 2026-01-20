from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import os
from dotenv import load_dotenv

load_dotenv()


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value



def get_db_connection():
    try:
        db_user = require_env("DB_USER")
        db_password = require_env("DB_PASSWORD")
        db_host = require_env("DB_HOST")
        db_name = require_env("DB_NAME")
        db_port = os.getenv("DB_PORT", "5432")

        db_url = (
            f"postgresql+psycopg2://{db_user}:{db_password}"
            f"@{db_host}:{db_port}/{db_name}"
        )

        engine = create_engine(
            db_url,
            echo=False,
            pool_pre_ping=True,
        )

        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        return engine

    except SQLAlchemyError as exc:
        print(f"Database connection failed: {exc}")
        return None

