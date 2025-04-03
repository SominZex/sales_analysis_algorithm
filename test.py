from sqlalchemy import create_engine, text
from connector import get_db_connection

engine = get_db_connection()

try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1;")) 
        print("Connection successful:", result.scalar()) 
except Exception as e:
    print("Database connection failed:", e)
finally:
    engine.dispose()
