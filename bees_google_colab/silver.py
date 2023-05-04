import os
import time
import json
from google.cloud import storage
from pyspark.sql import SparkSession

# Path to credentials file
creds_path = 'bees-385315-4e24c799eade.json'

# Load credentials from file
with open(creds_path, 'r') as f:
    creds_json = f.read()

creds = json.loads(creds_json)

# Create a client for Google Cloud Storage
storage_client = storage.Client.from_service_account_info(info=creds)

# Name of the bucket
bucket_name = 'teste_bees'

# Name of the source blob
source_blob_name = 'bronze/breweries.json'

# Local file name for the downloaded blob
destination_file_name = 'breweries.json'

# Download the source blob to a local file
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(source_blob_name)
blob.download_to_filename(destination_file_name)

# Create a Spark session
spark = SparkSession.builder.appName("Read breweries.json").getOrCreate()

# Read the downloaded JSON file into a Spark DataFrame
breweries_silver = spark.read.json("breweries.json")

# Partition and save the DataFrame in the Parquet format
breweries_silver.write.partitionBy('state').mode('overwrite').parquet('breweries_silver')

# Directory containing the partitioned data
local_directory_path = 'breweries_silver'

# Name of the directory in the bucket where the files will be stored
bucket_directory_name = 'silver'

# Iterate over each file in the local directory and upload it to the bucket
for root, dirs, files in os.walk(local_directory_path):
    for file_name in files:
        local_file_path = os.path.join(root, file_name)
        remote_file_path = os.path.join(bucket_directory_name, file_name)
        blob = storage_client.bucket(bucket_name).blob(remote_file_path)
        blob.upload_from_filename(local_file_path, timeout=600)
        print(f'File {local_file_path} uploaded to bucket {bucket_name} as {remote_file_path}')
        time.sleep(1) # pause for 1 second between uploads

print('Upload complete.')
