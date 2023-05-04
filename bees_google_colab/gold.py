from pyspark.sql import SparkSession
import os
import time
from google.cloud import storage
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder.appName("Read partitioned data").getOrCreate()

# Read the partitioned data
breweries_gold = spark.read.format("parquet").option("basePath", "breweries_silver").load("breweries_silver")

# Aggregate data by state and brewery type, count number of breweries per group, and sort by count in descending order
breweries_gold = breweries_gold.groupBy("state", "brewery_type").agg(count("*").alias("count")).orderBy("count", ascending=False)

# Write the data to a partitioned Parquet file
breweries_gold.write.partitionBy("state").mode("overwrite").parquet("breweries_gold")

# Path to credentials file
creds_path = 'bees-385315-4e24c799eade.json'

# Create a Google Cloud Storage client
storage_client = storage.Client.from_service_account_json(creds_path)

# Name of the bucket
bucket_name = 'teste_bees'

# Local directory containing files to be uploaded
local_directory_path = 'breweries_gold'

# Name of the directory in the bucket where files will be stored
bucket_directory_name = 'gold'

# Iterate over each file in the local directory and upload to the bucket
for root, dirs, files in os.walk(local_directory_path):
    for file_name in files:
        local_file_path = os.path.join(root, file_name)
        remote_file_path = os.path.join(bucket_directory_name, file_name)
        blob = storage_client.bucket(bucket_name).blob(remote_file_path)
        blob.upload_from_filename(local_file_path, timeout=600)
        print(f'File {local_file_path} uploaded to bucket {bucket_name} as {remote_file_path}')
        time.sleep(1) # Pause for 1 second between uploads
print('Upload complete.')