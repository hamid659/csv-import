import requests
import logging
import csv
import argparse
import sys
from datetime import datetime
from io import StringIO
import psycopg2
import yaml

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log', mode='a'),  # Log to file
        logging.StreamHandler()            # Log to console
    ]
)

def read_config(file_path):
    try:
        with open(file_path, 'r') as f:
            config = yaml.safe_load(f)
        logging.info("Database configuration read successfully.")
        return config['database']
    except Exception as e:
        logging.error(f"Error reading YAML file: {e}")
        return None

def check_line_format(fields):
    if len(fields) != 9:
        return False

    unique_id = fields[6].strip()
    artist_clean = fields[3].strip()

    if not unique_id or not artist_clean:
        return False

    return True

def detect_duplicates(valid_rows):
    seen_ids = set()
    duplicates = []

    for fields in valid_rows:
        unique_id = fields[6]
        if unique_id in seen_ids:
            duplicates.append(fields)
        else:
            seen_ids.add(unique_id)

    return duplicates

def import_csv_and_detect_duplicates(url, remove_duplicates=False, db_config=None, pre_analysis=False):
    connection = None
    try:
        connection = psycopg2.connect(
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port']
        )

        response = requests.get(url)
        response.raise_for_status()  # Ensure the request was successful
        data = response.text.replace('\r\n', '\n').replace('\r', '\n')
        csv_reader = csv.reader(StringIO(data), delimiter=',', quotechar='"')

        first_line = next(csv_reader, None)
        if first_line is None:
            logging.error("The CSV file is empty.")
            return

        expected_header = ["SONG RAW", "Song Clean", "ARTIST RAW", "ARTIST CLEAN", "CALLSIGN", "TIME", "UNIQUE_ID", "COMBINED", "First?"]
        if first_line != expected_header:
            logging.error(f"Unexpected header format: {first_line}")
            return

        valid_rows = []
        bad_data_found = False
        artists = set()  # To collect unique artists

        report_filename = f'bad_data_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

        for fields in csv_reader:
            if not check_line_format(fields):
                bad_data_found = True
                with open(report_filename, 'a', newline='') as report_file:
                    report_writer = csv.writer(report_file)
                    report_writer.writerow([','.join(fields), len(fields)])
                continue

            unique_id = fields[6]
            artist_clean = fields[3].strip()
            artists.add(artist_clean)  # Collect unique artists

            valid_rows.append(fields)

        if bad_data_found:
            logging.info(f"Bad data report created: {report_filename}")

        # Detect duplicates
        duplicates = detect_duplicates(valid_rows)

        if duplicates:
            logging.info(f"Found {len(duplicates)} duplicate rows based on 'UNIQUE_ID':")
            for dup in duplicates:
                logging.debug(f"Duplicate row: {dup}")

        if remove_duplicates:
            logging.info("Removing duplicates from valid rows.")
            valid_rows = [line for line in valid_rows if line[6] not in {dup[6] for dup in duplicates}]

        # Save cleaned data if not in pre-analysis mode
        if not pre_analysis:
            with open('cleaned_data.csv', 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(expected_header)
                writer.writerows(valid_rows)
            logging.info("Cleaned data saved to 'cleaned_data.csv'.")

            # Insert unique artists into the database
            insert_artists_to_db(artists, connection)
            # Fetch artist mapping
            artist_mapping = fetch_artist_mapping(connection)

            # Now, insert songs
            insert_songs_to_db(valid_rows, artist_mapping, connection)

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching CSV from GitHub: {e}")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Database error: {error}")
    finally:
        if connection is not None:
            connection.close()
            logging.info("Database connection closed.")

def insert_artists_to_db(artists, connection):
    cursor = None  # Initialize cursor to avoid UnboundLocalError
    try:
        cursor = connection.cursor()

        # Insert unique artists
        for artist_name in artists:
            insert_query = '''
            INSERT INTO artists (artist_name_raw, artist_name_clean)
            VALUES (%s, %s)
            ON CONFLICT (artist_name_clean) DO NOTHING;  -- Avoid duplicates based on artist_name_clean
            '''
            cursor.execute(insert_query, (artist_name, artist_name))

        connection.commit()
        logging.info(f"Inserted {len(artists)} unique artists into the artists table.")

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error inserting artists into database: {error}")

    finally:
        if cursor is not None:
            cursor.close()

def fetch_artist_mapping(connection):
    artist_mapping = {}
    cursor = None  # Initialize cursor to avoid UnboundLocalError
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT artist_id, artist_name_clean FROM artists;")
        artists = cursor.fetchall()
        for artist_id, artist_name_clean in artists:
            artist_mapping[artist_name_clean] = artist_id
        logging.info("Artist mapping created successfully.")
    except Exception as e:
        logging.error(f"Error fetching artist mapping: {e}")
    finally:
        if cursor is not None:
            cursor.close()
    return artist_mapping

def insert_songs_to_db(valid_rows, artist_mapping, connection):
    cursor = None  # Initialize cursor to avoid UnboundLocalError
    try:
        cursor = connection.cursor()

        # Insert songs
        for fields in valid_rows:
            song_name_raw = fields[0]         # SONG RAW
            song_name_clean = fields[1]       # Song Clean
            callsign = fields[4]         # CALLSIGN
            time = fields[5]             # TIME
            unique_id = fields[6]        # UNIQUE_ID
            combined = fields[7]         # COMBINED
            first_play = fields[8]            # First?

            # Get the artist ID from the mapping using the artist_clean field
            artist_id = artist_mapping.get(fields[3].strip())
            
            if artist_id is None:
                logging.warning(f"Artist not found for the song '{song_name_clean}'. Skipping insertion.")
                continue  # Skip if artist ID not found

            insert_query = '''
                INSERT INTO songs (song_name_raw, song_name_clean, artist_id, callsign, time, unique_id, combined, first_play)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            '''
            cursor.execute(insert_query, (song_name_raw, song_name_clean, artist_id, callsign, time, unique_id, combined, first_play))

        connection.commit()
        logging.info("Songs inserted successfully.")

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error inserting songs into database: {error}")

    finally:
        if cursor is not None:
            cursor.close()

def main():
    parser = argparse.ArgumentParser(description="Import CSV and detect/remove duplicates.")
    parser.add_argument('--url', type=str, required=True, help='The raw GitHub URL for the CSV.')
    parser.add_argument('--no-duplicate', action='store_true', help='Remove duplicates instead of just reporting them.')
    parser.add_argument('--config', type=str, default="config.yaml",  help='Path to the database configuration YAML file.')
    parser.add_argument('--pre-analysis', action='store_true', help='Only perform pre-analysis and report bad data.')

    args = parser.parse_args()

    if not args.url:
        logging.error("No URL provided. Please provide a raw GitHub URL for the CSV.")
        sys.exit(1)

    # Read database configuration from the provided YAML file
    db_config = read_config(args.config)
    if db_config is None:
        logging.error("Database configuration could not be read. Exiting.")
        sys.exit(1)

    import_csv_and_detect_duplicates(args.url, remove_duplicates=args.no_duplicate, db_config=db_config, pre_analysis=args.pre_analysis)

if __name__ == "__main__":
    main()
