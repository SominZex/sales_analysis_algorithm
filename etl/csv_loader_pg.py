import pandas as pd
import psycopg2
import psycopg2.extras
import time
import sys

class FastCSVLoader:
    def __init__(self):
        self.db_config = {
            "host": "server_ip_address",
            "port": "port_no",
            "database": "db_name",
            "user": "user_name",
            "password": "pw"
        }

    def load_csv_to_database(self, csv_file_path):
        """Load CSV directly to PostgreSQL - fastest method"""
        start_time = time.time()
        
        try:
            # Load CSV
            print(f"Loading CSV: {csv_file_path}")
            df = pd.read_csv(csv_file_path)
            print(f"Loaded {len(df)} rows")
            
            load_time = time.time()
            print(f"CSV loaded in {load_time - start_time:.2f} seconds")
            
            # Connect to database
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            
            # Get column names from DataFrame
            columns = list(df.columns)
            cols_str = ",".join([f'"{col}"' for col in columns])
            
            # Convert to list of tuples for bulk insert
            data_tuples = [tuple(row) for row in df.values]
            
            # Bulk insert using execute_values (fastest method)
            insert_sql = f'INSERT INTO billing_data ({cols_str}) VALUES %s'
            
            print(f"Inserting {len(data_tuples)} rows...")
            
            psycopg2.extras.execute_values(
                cur,
                insert_sql,
                data_tuples,
                page_size=2000  # Larger page size for speed
            )
            
            conn.commit()
            cur.close()
            conn.close()
            
            end_time = time.time()
            print(f"Database insert completed in {end_time - load_time:.2f} seconds")
            print(f"Total time: {end_time - start_time:.2f} seconds")
            print(f"Successfully inserted {len(df)} rows into billing_data")
            
            return True
            
        except Exception as e:
            print(f"Error: {e}")
            return False

def main():
    loader = FastCSVLoader()
    
    # Get CSV file path
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    else:
        csv_path = input("Enter CSV file path: ").strip()
    
    if csv_path:
        success = loader.load_csv_to_database(csv_path)
        if success:
            print("✓ CSV loaded successfully!")
        else:
            print("✗ Failed to load CSV")
    else:
        print("No file path provided")

if __name__ == "__main__":
    main()