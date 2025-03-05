from sqlalchemy import create_engine

def get_db_connection():
    """Returns an SQLAlchemy engine for MySQL connection."""
    engine = create_engine("mysql+pymysql://root:@localhost/sales_data")
    return engine
