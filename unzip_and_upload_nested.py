import os
import zipfile
import bz2
import shutil
import json
import time
import concurrent.futures
from google.cloud import storage
from google.resumable_media import requests as resumable_requests
from google.resumable_media import common
from google.auth.transport.requests import AuthorizedSession
from requests.exceptions import RequestException

# Initialize Google Cloud Storage client
storage_client = storage.Client()

def unzip_and_upload_local(local_directory, bucket_name, max_workers=10):
    """Recursively unzip all ZIP and BZ2 files in the local directory and upload contents to Google Cloud Storage."""

    # Get the Google Cloud Storage bucket
    bucket = storage_client.bucket(bucket_name)

    # Load the set of uploaded files from a JSON file, if it exists
    uploaded_files_path = 'uploaded_files.json'
    if os.path.exists(uploaded_files_path):
        with open(uploaded_files_path, 'r') as f:
            uploaded_files = set(json.load(f))
    else:
        uploaded_files = set()

    def process_directory(directory):
        files_to_process = []
        for root, _, files in os.walk(directory):
            if '__MACOSX' in root:
                continue
            for file_name in files:
                if file_name.startswith('.') or file_name.startswith('._'):
                    continue
                file_path = os.path.join(root, file_name)
                relative_path = os.path.relpath(file_path, local_directory)
                if relative_path not in uploaded_files:
                    files_to_process.append(file_path)
                else:
                    print(f'Skipping already uploaded file: {file_path}')
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(process_file, files_to_process)

    def process_file(file_path):
        file_name = os.path.basename(file_path)
        if file_name.endswith('.zip'):
            print(f'Processing ZIP file: {file_path}')
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(os.path.dirname(file_path))
            print(f'Extracted ZIP file: {file_path}')
            os.remove(file_path)
            print(f'Removed ZIP file after extraction: {file_path}')
            process_directory(os.path.dirname(file_path))
        elif file_name.endswith('.bz2'):
            print(f'Processing BZ2 file: {file_path}')
            decompressed_file = file_path[:-4]
            with bz2.open(file_path, 'rb') as source, open(decompressed_file, 'wb') as dest:
                shutil.copyfileobj(source, dest)
            print(f'Decompressed BZ2 file: {file_path}')
            os.remove(file_path)
            print(f'Removed BZ2 file after decompression: {file_path}')
            upload_file(decompressed_file)
        else:
            upload_file(file_path)

    def upload_file(file_path):
        relative_path = os.path.relpath(file_path, local_directory)
        
        if relative_path not in uploaded_files:
            destination_blob = bucket.blob(relative_path)
            transport = AuthorizedSession(credentials=storage_client._credentials)
            chunk_size = 1 * 1024 * 1024  # 1MB chunks

            # Determine the content type
            if file_path.lower().endswith(('.h5', '.hdf5')):
                content_type = 'application/x-hdf5'
            else:
                content_type = 'application/octet-stream'

            try:
                resumable_upload = resumable_requests.ResumableUpload(
                    upload_url=f"https://www.googleapis.com/upload/storage/v1/b/{bucket_name}/o?uploadType=resumable",
                    chunk_size=chunk_size
                )

                with open(file_path, 'rb') as file_obj:
                    metadata = {'name': relative_path}
                    response = resumable_upload.initiate(
                        transport,
                        file_obj,
                        metadata,
                        content_type=content_type,
                    )

                    while not resumable_upload.finished:
                        response = resumable_upload.transmit_next_chunk(transport)

                print(f'Uploaded {file_path} to {relative_path} in bucket {bucket_name}')

                uploaded_files.add(relative_path)
                with open(uploaded_files_path, 'w') as f:
                    json.dump(list(uploaded_files), f)

            except (RequestException, common.DataCorruption) as e:
                print(f"Error uploading {file_path}: {str(e)}")
                print("Retrying in 5 seconds...")
                time.sleep(5)
                upload_file(file_path)  # Retry the upload

        else:
            print(f'Skipped duplicate file: {file_path}')

    # Start processing the directory recursively
    process_directory(local_directory)


if __name__ == '__main__':
    local_directory = "ENTER LOCAL DIRECTORY HERE"
    bucket_name = "ENTER BUCKET NAME HERE"
    unzip_and_upload_local(local_directory, bucket_name)


