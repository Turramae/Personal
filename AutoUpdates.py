import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from tqdm import tqdm
from pydomo import Domo
import tempfile
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='auto_updates.log',
    filemode='a'
)

# Domo authentication
client_id = '152fe71a-bc33-42b3-a38b-e0e82615f71f'
client_secret = 'a5adb7f9e13561d3371971fa6883639c73c87189cdb626f363aad2449b487495'

try:
    domo = Domo(client_id, client_secret, api_host='api.domo.com')
    logging.info("Authenticated with Domo successfully.")
except Exception as e:
    logging.critical(f"Failed to authenticate with Domo: {e}")
    exit(1)

DATASET_ID = 'NOAA Storm Events'

# Load list of already uploaded files
log_file = 'uploaded_files.txt'
if not os.path.exists(log_file):
    with open(log_file, 'w') as f:
        pass

with open(log_file, 'r') as f:
    already_uploaded = set(line.strip() for line in f)

# Scrape NOAA directory for .csv.gz files
base_url = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/'

try:
    response = requests.get(base_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    file_links = [
        base_url + node.get('href')
        for node in soup.find_all('a')
        if node.get('href') and node.get('href').endswith('.csv.gz')
    ]
    logging.info(f"Scraped {len(file_links)} total CSV files from NOAA site.")
except Exception as e:
    logging.critical(f"Error scraping NOAA directory: {e}")
    exit(1)

# Filter only new files
new_files = [url for url in file_links if url.split('/')[-1] not in already_uploaded]
logging.info(f"Found {len(new_files)} new files to process.")

if not new_files:
    logging.info("No new files to upload. Exiting.")
    exit()

# Download, combine, and upload new data
df_list = []

for url in tqdm(new_files, desc="Processing new files"):
    filename = url.split('/')[-1]
    try:
        logging.info(f"Downloading {filename}")
        df = pd.read_csv(url, compression='gzip', low_memory=False)
        df['load_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # ✅ Add timestamp here

        print(df.columns)
        
        df_list.append(df)
        with open(log_file, 'a') as f:
            f.write(filename + '\n')
        logging.info(f"Successfully downloaded and parsed {filename} with {len(df)} rows.")
    except Exception as e:
        logging.warning(f"Error reading {filename}: {e}")

if df_list:
    try:
        new_data = pd.concat(df_list, ignore_index=True)
        logging.info(f"Uploading {new_data.shape[0]} rows to Domo dataset ID: {DATASET_ID}")

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmpfile:
            new_data.to_csv(tmpfile.name, index=False)
            domo.datasets.data_import_from_file(DATASET_ID, tmpfile.name, update_method='REPLACE')

        logging.info("Upload to Domo complete.")
    except Exception as e:
        logging.error(f"Failed to upload data to Domo: {e}")
else:
    logging.info("No new data to upload after file processing.")
