from google.cloud import storage
import os
from google.oauth2 import service_account

def upload_to_bucket(bucket_name, source_folder, credentials_path):
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    
    for local_file in os.listdir(source_folder):
        local_path = os.path.join(source_folder, local_file)
        
        if os.path.isfile(local_path):
            blob = bucket.blob(local_file)
            blob.upload_from_filename(local_path)
            print(f"{local_file} uploaded to {bucket_name}.")
        else:
            print(f"{local_file} is not a file and was skipped.")

BUCKET_NAME = 'inD'
SOURCE_FOLDER = 'path/to/your/local/dataset'
CREDENTIALS_PATH = 'path/to/your/google-credentials-file.json'

upload_to_bucket(BUCKET_NAME, SOURCE_FOLDER, CREDENTIALS_PATH)
