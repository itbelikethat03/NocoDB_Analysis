import os
import pymysql
from pymysql.constants import CLIENT

# Database connection configuration
db_config = {
    "host": "localhost",
    "port": 3310,
    "user": "root",
    "password": "brin",
    "database": "transactions_db",
    "client_flag": CLIENT.LOCAL_FILES,
    "connect_timeout": 60,
    "read_timeout": 60 * 60,  # 1 hour
    "write_timeout": 60 * 60  # 1 hour
}

# Path to the directory containing the CSV files
source_dir = r"D:\TPO\sample_mflix\transactions"

# Get all CSV files from the directory
files_to_insert = sorted([
    os.path.join(source_dir, file) 
    for file in os.listdir(source_dir) 
    if file.endswith('.csv')
])

def get_file_size(file_path):
    """Get file size in MB"""
    return os.path.getsize(file_path) / (1024 * 1024)

def process_file(file_path, connection, cursor):
    """Process a single file with error handling"""
    try:
        file_size = get_file_size(file_path)
        file_name = os.path.basename(file_path)
        print(f"\nStarting import of {file_name} ({file_size:.2f} MB)")
        
        # Convert Windows path to MySQL format
        mysql_path = file_path.replace("\\", "/")
        
        query = f"""
        LOAD DATA INFILE '{mysql_path}'
        INTO TABLE transactions_data
        FIELDS TERMINATED BY ','
        ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 ROWS
        (id, date, client_id, card_id, amount, use_chip, merchant_id,
         merchant_city, merchant_state, zip, mcc, errors);
        """
        
        cursor.execute(query)
        connection.commit()
        
        print(f"✓ Successfully imported {file_name}")
        return True
        
    except pymysql.Error as e:
        print(f"MySQL Error while processing {os.path.basename(file_path)}: {e}")
        connection.rollback()
        return False
        
    except Exception as e:
        print(f"Unexpected error while processing {os.path.basename(file_path)}: {e}")
        connection.rollback()
        return False

def main():
    if not files_to_insert:
        print(f"No CSV files found in {source_dir}")
        return

    print(f"Found {len(files_to_insert)} CSV files to process")


    
    failed_files = []
    successful_files = []
    
    try:
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()
        
        # First, let's verify the table exists
        try:
            cursor.execute("SHOW CREATE TABLE transactions_data")
        except pymysql.Error:
            print("Creating transactions_data table...")
            create_table_query = """
            CREATE TABLE IF NOT EXISTS transactions_data (
                id VARCHAR(255),
                date DATETIME,
                client_id VARCHAR(255),
                card_id VARCHAR(255),
                amount DECIMAL(10,2),
                use_chip VARCHAR(10),
                merchant_id VARCHAR(255),
                merchant_city VARCHAR(255),
                merchant_state VARCHAR(255),
                zip VARCHAR(10),
                mcc VARCHAR(10),
                errors VARCHAR(255),
                PRIMARY KEY (id)
            )
            """
            cursor.execute(create_table_query)
            connection.commit()
        
        for file_path in files_to_insert:
            success = process_file(file_path, connection, cursor)
            if success:
                successful_files.append(file_path)
            else:
                failed_files.append(file_path)
                
    except pymysql.Error as e:
        print(f"Database connection error: {e}")
    finally:
        if 'connection' in locals():
            connection.close()
    
    # Print summary
    print("\nImport Summary:")
    print(f"Successfully imported: {len(successful_files)} files")
    print(f"Failed to import: {len(failed_files)} files")
    
    if failed_files:
        print("\nFailed files:")
        for file in failed_files:
            print(f"- {os.path.basename(file)}")

if __name__ == "__main__":
    main()
