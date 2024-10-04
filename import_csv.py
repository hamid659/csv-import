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

def fetch_csv_data(url):
    response = requests.get(url)
    response.raise_for_status()  # Ensure the request was successful
    data = response.text.replace('\r\n', '\n').replace('\r', '\n')
    return data

def validate_header(first_line):
    expected_header = [
        "SONG RAW", "Song Clean", "ARTIST RAW", 
        "ARTIST CLEAN", "CALLSIGN", "TIME", 
        "UNIQUE_ID", "COMBINED", "First?"
    ]
    return first_line == expected_header

def process_csv_data(csv_reader, handle_bad_data, connection):
    valid_rows = []
    artists = set()  # To collect unique artists
    bad_data_found = False

    report_filename = f'bad_data_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

    for fields in csv_reader:
        if not check_line_format(fields):
            bad_data_found = True
            if handle_bad_data == 'report':
                log_bad_data(fields, report_filename)
            else:
                insert_bad_data(fields, connection)
            continue # Skip to the next row if bad data

        artist_clean = fields[3].strip()
        artists.add(artist_clean)  # Collect unique artists
        valid_rows.append(fields)

    if bad_data_found:
        logging.info(f"Bad data report created: {report_filename}")

    return valid_rows, artists

def log_bad_data(fields, report_filename):
    with open(report_filename, 'a', newline='') as report_file:
        report_writer = csv.writer(report_file)
        report_writer.writerow([','.join(fields), len(fields)])

# create an unknown artist id in the artists tble for songs without an artist name 
def create_unknown_artist(connection):
    try:
        with connection.cursor() as cursor:
            # Check if 'unknown' artist exists
            cursor.execute("SELECT artist_id FROM artists WHERE artist_name_clean = 'unknown';")
            result = cursor.fetchone()
            if result:
                return result[0]  # Return the artist_id

            # Insert 'unknown' artist if it doesn't exist
            insert_query = '''
                INSERT INTO artists (artist_name_raw, artist_name_clean)
                VALUES ('unknown', 'unknown')
                RETURNING artist_id;
            '''
            cursor.execute(insert_query)
            return cursor.fetchone()[0]  # Return the new artist_id
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error handling unknown artist: {error}")
        return None
    
def import_csv(url, remove_duplicates=False, db_config=None, pre_analysis=False, handle_bad_data='report'):
    connection = None
    try:
        connection = psycopg2.connect(
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port']
        )

        data = fetch_csv_data(url)
        csv_reader = csv.reader(StringIO(data), delimiter=',', quotechar='"')

        first_line = next(csv_reader, None)
        if first_line is None:
            logging.error("The CSV file is empty.")
            return

        if not validate_header(first_line):
            logging.error(f"Unexpected header format: {first_line}")
            return

        valid_rows, artists = process_csv_data(csv_reader, handle_bad_data, connection)

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
            save_cleaned_data(valid_rows)
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

def save_cleaned_data(valid_rows):
    expected_header = [
        "SONG RAW", "Song Clean", "ARTIST RAW", 
        "ARTIST CLEAN", "CALLSIGN", "TIME", 
        "UNIQUE_ID", "COMBINED", "First?"
    ]
    
    with open('cleaned_data.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(expected_header)
        writer.writerows(valid_rows)
    logging.info("Cleaned data saved to 'cleaned_data.csv'.")

def insert_bad_data(fields, connection):
    unknown_artist_id = create_unknown_artist(connection)
    if unknown_artist_id:
        # Prepare fields for insertion, link to unknown artist
        song_name_raw = fields[0]
        song_name_clean = fields[1]
        callsign = fields[4]
        time = fields[5]
        unique_id = fields[6]
        combined = fields[7]
        first_play = fields[8]

        insert_query = '''
            INSERT INTO songs (song_name_raw, song_name_clean, artist_id, callsign, time, unique_id, combined, first_play)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        '''
        with connection.cursor() as cursor:
            cursor.execute(insert_query, (song_name_raw, song_name_clean, unknown_artist_id, callsign, time, unique_id, combined, first_play))
        connection.commit()
        logging.info(f"Inserted bad data linked to unknown artist ID for song: '{song_name_clean}'.")

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
            callsign = fields[4]              # CALLSIGN
            time = fields[5]                  # TIME
            unique_id = fields[6]             # UNIQUE_ID
            combined = fields[7]              # COMBINED
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
    parser.add_argument('--config', type=str, default="config.yaml", help='Path to the database configuration YAML file.')
    parser.add_argument('--pre-analysis', action='store_true', help='Only perform pre-analysis and report bad data.')
    parser.add_argument('--handle-bad-data', choices=['report', 'insert'], default='report', 
                        help='Specify how to handle bad data: "report" to log to a file, "insert" to save to the database.')

    args = parser.parse_args()

    if not args.url:
        logging.error("No URL provided. Please provide a raw GitHub URL for the CSV.")
        sys.exit(1)

    # Read database configuration from the provided YAML file
    db_config = read_config(args.config)
    if db_config is None:
        logging.error("Database configuration could not be read. Exiting.")
        sys.exit(1)

    import_csv(args.url, remove_duplicates=args.no_duplicate, db_config=db_config, pre_analysis=args.pre_analysis, handle_bad_data=args.handle_bad_data)

if __name__ == "__main__":
    main()
