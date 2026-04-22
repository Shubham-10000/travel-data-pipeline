import requests
import os
from dotenv import load_dotenv
import json
from datetime import datetime
import time
import boto3

# =========================
# LOAD ENV VARIABLES
# =========================
load_dotenv()

API_KEY = os.getenv('API_KEY')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')

# =========================
# VALIDATION (IMPORTANT)
# =========================
if not API_KEY:
    raise ValueError("API_KEY not found in .env")

if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
    raise ValueError("AWS credentials not found in .env")

# =========================
# CREATE S3 CLIENT
# =========================
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name='ap-south-1'
)

bucket_name = 'travel-data-pipeline-bunny'
city = 'Mumbai'

# =========================
# INGESTION LOOP
# =========================
for _ in range(20):

    try:
        url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric'
        response = requests.get(url, timeout=10)

        if response.status_code == 200:
            data = response.json()

            weather_data = {
                'city': data['name'],
                'temperature': data['main']['temp'],
                'humidity': data['main']['humidity'],
                'weather': data['weather'][0]['description'],
                'timestamp': datetime.now().isoformat()
            }

            # =========================
            # LOCAL FILE (APPEND MODE)
            # =========================
            filename = f"weather_{datetime.now().date()}.json"

            with open(filename, 'a') as f:
                json.dump(weather_data, f)
                f.write("\n")

            # =========================
            # UNIQUE S3 KEY (BEST PRACTICE)
            # =========================
            key = f"weather-data/{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.json"

            # =========================
            # UPLOAD TO S3
            # =========================
            s3.upload_file(
                Filename=filename,
                Bucket=bucket_name,
                Key=key
            )

            print(f"[{datetime.now()}] ✅ Saved + Uploaded → {key}")

        else:
            print(f"[ERROR] API Failed: {response.status_code}")

    except Exception as e:
        print(f"[EXCEPTION] {str(e)}")

    time.sleep(5)

print("🚀 Pipeline execution completed")