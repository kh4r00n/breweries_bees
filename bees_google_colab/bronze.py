import requests
import json
from google.cloud import storage

# API URL
url = 'https://api.openbrewerydb.org/v1/breweries'

# List to store the breweries
all_breweries = []
page = 1

# Loop to collect all breweries using pagination
while True:
    response = requests.get(f'{url}?page={page}&per_page=50')
    data = json.loads(response.text)

    # Check if there is more data to be collected
    if not data:
        break

    all_breweries.extend(data)
    page += 1

print(f'The file contains {len(all_breweries)} beers/lines/rows.')

# Create the storage_client object
storage_client = storage.Client.from_service_account_json('bees-385315-4e24c799eade.json')

# Create the bucket object
bucket = storage_client.bucket('teste_bees')

# Create the blob object
blob = bucket.blob('bronze/breweries.json')

# Save the breweries to the blob
blob.upload_from_string(json.dumps(all_breweries))

print(f'The breweries.json file was saved to the bucket gs://teste_bees/bronze.')
