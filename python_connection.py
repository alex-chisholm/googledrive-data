import os
import json
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload  # Import this line
from googleapiclient.discovery import build
import pandas as pd
import duckdb
from google.cloud import storage
import io


# Step 1: Load environment variables from .env file
load_dotenv()

# Step 2: Reconstruct JSON credentials from environment variables
credentials_dict = {
    "type": "service_account",
    "project_id": os.getenv("GOOGLE_PROJECT_ID"),
    "private_key_id": os.getenv("GOOGLE_PRIVATE_KEY_ID"),
    "private_key": os.getenv("GOOGLE_PRIVATE_KEY").replace('\\n', '\n'),  # Ensure line breaks in private key
    "client_email": os.getenv("GOOGLE_CLIENT_EMAIL"),
    "client_id": os.getenv("GOOGLE_CLIENT_ID"),
    "auth_uri": os.getenv("GOOGLE_AUTH_URI"),
    "token_uri": os.getenv("GOOGLE_TOKEN_URI"),
    "auth_provider_x509_cert_url": os.getenv("GOOGLE_AUTH_PROVIDER_CERT_URL"),
    "client_x509_cert_url": os.getenv("GOOGLE_CLIENT_CERT_URL"),
}


#### google drive via service account (duck db won't query)

# Step 3: Authenticate using the credentials
credentials = service_account.Credentials.from_service_account_info(credentials_dict)

# Step 4: Use Google Drive API to download a file
drive_service = build('drive', 'v3', credentials=credentials)

file_id = "0Bx40g4v5or5ialVvWkZRVmV6STQ"  # Replace with your file's Google Drive ID
request = drive_service.files().get_media(fileId=file_id)
with open('temp.csv', 'wb') as f:
    downloader = MediaIoBaseDownload(f, request) # but we want to avoid downloading!
    done = False
    while not done:
        _, done = downloader.next_chunk()
# Step 5: Read the CSV
df = pd.read_csv("temp.csv")
print(df.head())



##### google cloud storage

# Step 3: Authenticate and initialize GCS client
client = storage.Client.from_service_account_info(credentials_dict)

# Step 4: Access the file in GCS bucket
bucket_name = "data-access-alex"  # Replace with your GCS bucket name
file_name = "recipes.csv"  # Replace with your file path in the bucket

bucket = client.bucket(bucket_name)
blob = bucket.blob(file_name)

# Read the file content directly into memory
file_stream = io.BytesIO()
blob.download_to_file(file_stream)
file_stream.seek(0)  # Reset the stream position to the beginning

# Step 5: Read the CSV from the in-memory file stream
df = pd.read_csv(file_stream)
df.query("Name == 'Lemonade'")


df


#### duck db with GCS


# Create a signed URL to access the file
signed_url = blob.generate_signed_url(version="v4", expiration=3600)  # URL valid for 1 hour

# Step 5: Query the data with DuckDB directly from the signed URL
query = f"""
    SELECT * FROM read_csv_auto('{signed_url}')
    LIMIT 10
"""

# DuckDB query
result = duckdb.query(query).to_df()

# Step 6: Display the queried result
print(result)





### big data 


conn = duckdb.connect()

# Path to your large CSV file
csv_file_path = 'path/to/your/large_file.csv'

# Read the CSV file using DuckDB's read_csv_auto function
# This function infers the schema and reads the data in chunks
conn.execute(f"CREATE VIEW large_data AS SELECT * FROM read_csv_auto('{csv_file_path}')")

# Retrieve the column names from the CSV
column_names = conn.execute("SELECT column_name FROM duckdb_columns() WHERE table_name = 'large_data'").fetchall()

# Initialize a list to store columns containing 'Lemonade'
columns_with_lemonade = []

# Iterate over each column and check for the presence of 'Lemonade'
for column in column_names:
    col_name = column[0]
    # Construct and execute a query to check for 'Lemonade' in the current column
    result = conn.execute(f"SELECT COUNT(*) FROM large_data WHERE {col_name} LIKE '%Lemonade%'").fetchone()
    # If the count is greater than 0, 'Lemonade' is present in this column
    if result[0] > 0:
        columns_with_lemonade.append(col_name)

# Output the columns that contain 'Lemonade'
print("Columns containing 'Lemonade':", columns_with_lemonade)