import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from tqdm import tqdm
from pydomo import Domo
import tempfile

# Setup
client_id = '152fe71a-bc33-42b3-a38b-e0e82615f71f'
client_secret = 'a5adb7f9e13561d3371971fa6883639c73c87189cdb626f363aad2449b487495'
domo = Domo(client_id, client_secret, api_host='api.domo.com')

# Domo dataset ID
DATASET_ID = 'NOAA Storm Events'

# Log file to track downloaded files
log_file = 'uploaded_files.txt'
if not os.path.exists(log_file):
    with open(log_file, 'w') as f:
        pass

with open(log_file, 'r') as f:
    already_uploaded = set(line.strip() for line in f)

# Scrape NOAA Directory
base_url = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/'
response = requests.get(base_url)
soup = BeautifulSoup(response.text, 'html.parser')

file_links = [
    base_url + node.get('href') 
    for node in soup.find_all('a') 
    if node.get('href') and node.get('href').endswith('.csv.gz')
]

# Filter new files only
new_files = [url for url in file_links if url.split('/')[-1] not in already_uploaded]
print(f"🆕 Found {len(new_files)} new files.")

if not new_files:
    print(" 🚫 No new files to upload.")
    exit()

# Download, combine and upload
df_list = []
for url in tqdm(new_files, desc="Processing new files"):
    filename = url.split('/')[-1]
    try:
        df = pd.read_csv(url, compression='gzip', low_memory=False)
        df_list.append(df)
        # Mark as uploaded
        with open(log_file, 'a') as f:
            f.write(filename + '\n')
    except Exception as e:
        print(f"⚠️ Error reading {filename}: {e}")

# Combine new data
if df_list:
    new_data = pd.concat(df_list, ignore_index=True)
    print(f"⬆ Uploading {new_data.shape[0]} rows to Domo...")

    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmpfile:
        new_data.to_csv(tmpfile.name, index=False)
        domo.datasets.data_import_from_file(DATASET_ID, tmpfile.name)

    print("✅ Upload complete.")
