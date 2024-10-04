# CSV to PostgreSQL Data Import Script

# Overview
This script is designed to import song data from a CSV file into a PostgreSQL database. It checks for data consistency, removes duplicates (if configured), and inserts both artist and song information into corresponding tables in the database.

The CSV file should follow a specific format, and the script supports pre-analysis to detect bad data before importing. Additionally, duplicate rows based on a unique identifier can either be reported or removed.

# Features
- Fetches CSV file from a provided URL.
- **Pre-analysis mode** to check for bad data without inserting it into the database.
- **Detection and removal** of duplicate rows based on UNIQUE_ID.
- **Option to handle bad data**, allowing you to either report it in a file or insert it into the database linked to an "unknown" artist.
- Inserts unique artists into an artists table.
- Inserts songs into a songs table, mapping them to their respective artists.
- Logs actions and errors, appending them to a log file (app.log).

## Prerequisites
- Python 3.7
- PostgreSQL 17.0

## Install Dependencies
All necessary Python packages are listed in the requirements.txt file. To install them, run:
``` bash
pip install -r requirements.txt
```

## Database Schema
The script requires two PostgreSQL tables: artists and songs. 
To initialize the database tables, use the db_init.py script. This script will create the artists and songs tables.

If you want to recreate the tables (drop and create new ones), pass the --recreate flag:
``` bash
python db_init.py --recreate
```

# Usage
## Command Line Arguments
Argument	Description
- --url	The raw URL of the CSV file to import.
- --no-duplicate	Optional. If provided, the script will remove duplicate entries.
- --config	Required. Path to the YAML file containing database configuration.
- --pre-analysis	Optional. If provided, the script will perform pre-analysis only.
- --handle-bad-data (Optional): Specify how to handle bad data: use report to log it to a file, or insert to save it to the database. If you choose insert, the data with missing information will be added and linked to an "unknown" artist name.

### Example Command 
``` bash
python import_csv.py https://example.com/data.csv --config db_config.yaml --no-duplicate
```

### Sample YAML Configuration File (db_config.yaml)
The database configuration must be provided as a YAML file, structured like this:
``` yaml
database:
  host: "database_address"
  port: 5432
  user: "your_username"
  password: "your_password"
  dbname: "your_database_name"
```

### Pre-Analysis Mode
If you want to analyze the CSV data for bad rows or format issues without inserting it into the database, use the --pre-analysis flag. The script will generate a report for bad data but will not modify the database.
``` bash
python import_csv.py https://example.com/data.csv --config config.yaml --pre-analysis

```