import os
import json
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload  # Import this line
import pandas as pd
import duckdb


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

### duck db
con = duckdb.connect()
query = f"SELECT * FROM read_csv_auto('gdrive://{file_id}');"
result = con.execute(query).fetchdf()
