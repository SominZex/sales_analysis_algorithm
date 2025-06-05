from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

def get_db_connection():
    try:
        engine = create_engine("mysql+pymysql://root:root@localhost/sales_data")
        # Test connection properly with SQLAlchemy 2.x
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except SQLAlchemyError as e:
        print(f"Database connection failed: {e}")
        return None
