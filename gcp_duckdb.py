import os
import json
import duckdb
from google.auth import credentials
from google.cloud import storage
from google.auth import exceptions
from google.auth.transport.requests import Request
from google.auth import service_account

# Step 1: Build the credentials dict from environment variables
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

# Step 2: Use google-auth to load credentials from the environment variables
def get_credentials():
    # Convert the dictionary to a JSON string and load it as a credentials object
    credentials_info = json.dumps(credentials_dict)
    creds = service_account.Credentials.from_service_account_info(json.loads(credentials_info))
    
    return creds

# Step 3: Function to generate a signed URL
def generate_signed_url(bucket_name, file_path, expiration=3600):
    """
    Generate a signed URL to access a private GCS file.
    
    Parameters:
    bucket_name (str): GCS bucket name
    file_path (str): Path to the file in GCS
    expiration (int): Expiration time in seconds (default is 1 hour)
    
    Returns:
    str: The signed URL
    """
    # Get the credentials
    creds = get_credentials()

    # Initialize the Google Cloud Storage client with the credentials
    storage_client = storage.Client(credentials=creds, project=os.getenv("GOOGLE_PROJECT_ID"))
    
    # Get the bucket and the blob (file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    
    # Generate a signed URL for the file
    signed_url = blob.generate_signed_url(version="v4", expiration=expiration)  # URL valid for 1 hour
    
    return signed_url

# Step 4: Query function for DuckDB to read GCS CSV
def query_gcs_csv(bucket_name, file_path, sql_query):
    """
    Query a CSV file stored in Google Cloud Storage using DuckDB without downloading the file.
    
    Parameters:
    bucket_name (str): Name of the GCS bucket
    file_path (str): Path to the CSV file within the bucket
    sql_query (str): SQL query to execute against the CSV file
    
    Returns:
    pandas.DataFrame: Query results
    """
    # Generate signed URL
    signed_url = generate_signed_url(bucket_name, file_path)

    # Initialize DuckDB connection
    conn = duckdb.connect()

    # Install and load the HTTPFS extension to enable reading from remote files
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    
    # Use the signed URL for reading CSV from GCS
    conn.execute(f"""
        CREATE VIEW csv_data AS 
        SELECT * FROM read_csv_auto('{signed_url}')
    """)

    # Execute the SQL query on the CSV data
    result = conn.execute(sql_query).fetchdf()

    # Clean up by dropping the view
    conn.execute("DROP VIEW IF EXISTS csv_data")
    conn.close()

    # Return the query results as a pandas DataFrame
    return result


bucket_name = "data-access-alex"
file_path = "recipes.csv"
sql_query = """
    SELECT * 
    FROM csv_data 
    WHERE Name LIKE '%Lemonade%'
"""

sql_query = """
    SELECT * 
    FROM csv_data 
    WHERE Name = 'Lemonade'
"""

# Query the CSV file
result_df = query_gcs_csv(bucket_name, file_path, sql_query)

# Print the results
print(result_df)
