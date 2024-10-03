import yaml
import psycopg2
import logging
import argparse

# Function to read the database credentials from a YAML file
def read_db_config(file_path):
    try:
        with open(file_path, 'r') as f:
            config = yaml.safe_load(f)
        logging.info("Database configuration read successfully.")
        return config['database']
    except Exception as e:
        logging.error(f"Error reading YAML file: {e}")
        return None

# Function to execute SQL script from a file
def execute_sql_file(connection, sql_file):
    try:
        with open(sql_file, 'r') as f:
            sql = f.read()
        with connection.cursor() as cursor:
            cursor.execute(sql)
            connection.commit()
        logging.info("SQL script executed successfully.")
    except Exception as e:
        logging.error(f"Error executing SQL script: {e}")
        connection.rollback()

# Function to drop existing tables if required
def drop_existing_tables(connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS songs;")
            cursor.execute("DROP TABLE IF EXISTS artists;")
            connection.commit()
        logging.info("Existing tables dropped successfully.")
    except Exception as e:
        logging.error(f"Error dropping existing tables: {e}")

def main():
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description="Recreate tables in PostgreSQL.")
    parser.add_argument('--recreate', action='store_true', help='Drop existing tables before recreating them.')
    parser.add_argument('--config', type=str, default="config.yaml", help='Path to the database configuration YAML file.')

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('app.log', mode='a'),  # Log to file
            logging.StreamHandler()  # Log to console
        ]
    )

   # Read the database config from the specified config file
    db_config = read_db_config(args.config)
    if not db_config:
        logging.error("Database configuration could not be read.")
        return
    
    # Initialize the connection variable
    connection = None

    # Connect to PostgreSQL
    try:
        connection = psycopg2.connect(**db_config)  # Use the read config directly
        logging.info("Connection to PostgreSQL DB successful")

        # Drop existing tables if --recreate is specified
        if args.recreate:
            logging.info("Dropping existing tables...")
            drop_existing_tables(connection)

        # Execute the SQL script to create tables
        execute_sql_file(connection, "create_tables.sql")

    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
    finally:
        # Ensure connection is closed if it was successfully opened
        if connection is not None:
            connection.close()
            logging.info("PostgreSQL connection closed.")

if __name__ == "__main__":
    main()
