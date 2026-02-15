import pandas as pd
from connector import get_db_connection

def get_last_date():
    engine = get_db_connection()
    query = """
    SELECT MAX(orderDate) as last_date 
    FROM sales_data
    """
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df['last_date'].iloc[0]