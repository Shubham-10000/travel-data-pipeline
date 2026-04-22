import boto3
import json
import psycopg2
import os
from dotenv import load_dotenv
import logging

# =====================
# LOAD ENV VARIABLES
# =====================
load_dotenv()

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME', 'travel-data-pipeline-bunny')

# =====================
# SETUP LOGGING
# =====================
logging.basicConfig(
    filename='etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# =====================
# CREATE S3 CLIENT
# =====================
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name='ap-south-1'
)

# =====================
# DATABASE CONNECTION
# =====================
conn = psycopg2.connect(
    dbname="weather_db",
    user="bunny",
    password="1102",
    host="localhost",
    port="5432"
)

cursor = conn.cursor()

# =====================
# FETCH FILES FROM S3
# =====================
response = s3.list_objects_v2(
    Bucket=BUCKET_NAME,
    Prefix="weather-data/"
)

if 'Contents' not in response:
    logging.info("No files found in S3")
    exit()

# =====================
# PROCESS EACH FILE
# =====================
for obj in response['Contents']:
    key = obj['Key']

    # Skip non-json (safety)
    if not key.endswith(".json"):
        continue

    logging.info(f"Processing file: {key}")

    # =====================
    # CHECK IF FILE ALREADY PROCESSED
    # =====================
    cursor.execute(
        "SELECT 1 FROM processed_files WHERE file_key=%s",
        (key,)
    )
    if cursor.fetchone():
        logging.info(f"Skipping already processed file: {key}")
        continue

    # =====================
    # READ FILE FROM S3
    # =====================
    file_obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    content = file_obj['Body'].read().decode('utf-8')

    records = []

    # =====================
    # TRANSFORM DATA
    # =====================
    for line in content.strip().split("\n"):
        try:
            data = json.loads(line)

            records.append((
                data['city'],
                data['temperature'],
                data['humidity'],
                data['weather'],
                data['timestamp']
            ))

        except Exception as e:
            logging.error(f"Error parsing line: {e}")

    # =====================
    # LOAD DATA (BATCH INSERT)
    # =====================
    if records:
        cursor.executemany("""
            INSERT INTO weather_data (city, temperature, humidity, weather, timestamp)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (city, timestamp) DO NOTHING;
        """, records)

    # =====================
    # MARK FILE AS PROCESSED
    # =====================
    cursor.execute(
        "INSERT INTO processed_files (file_key) VALUES (%s)",
        (key,)
    )

    logging.info(f"Finished processing: {key}")

# =====================
# COMMIT & CLOSE
# =====================
conn.commit()
cursor.close()
conn.close()

logging.info("ETL pipeline completed successfully")
print("✅ Data loaded into PostgreSQL")