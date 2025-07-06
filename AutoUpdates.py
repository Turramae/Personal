import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from tqdm import tqdm
from pydomo import Domo
import tempfile
from datetime import datetime

# Setup
client_id = '152fe71a-bc33-42b3-a38b-e0e82615f71f'
client_secret = 'a5adb7f9e13561d3371971fa6883639c73c87189cdb626f363aad2449b487495'
domo = Domo(client_id, client_secret, api_host='api.domo.com')

# Dataset 
DATASET_ID = 'NOAA Storm Events'  

# Log tracking 
log_file = 'uploaded_files.txt'
if not os.path.exists(log_file):
    open(log_file, 'w').close()

with open(log_file, 'r') as f:
    already_uploaded = set(line.strip() for line in f)

#  Today's date 
today_str = datetime.utcnow().strftime('%Y-%b-%d')  

# Scrape NOAA Directory
base_url = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/'
response = requests.get(base_url)
soup = BeautifulSoup(response.text, 'html.parser')
rows = soup.find_all('tr')

# Find new files modified today and not already uploaded
file_links = []
for row in rows:
    cells = row.find_all('td')
    if len(cells) >= 2:
        link_tag = cells[0].find('a')
        date_text = cells[1].text.strip()
        if link_tag and link_tag.get('href', '').endswith('.csv.gz'):
            file_name = link_tag.get('href')
            if today_str in date_text and file_name not in already_uploaded:
                file_links.append(base_url + file_name)

print(f"🆕 Found {len(file_links)} new file(s) modified today ({today_str}).")

if not file_links:
    print("🚫 No new files to upload today.")
    exit()

#  Download and process files
df_list = []
for url in tqdm(file_links, desc="Processing today's new files"):
    filename = url.split('/')[-1]
    try:
        df = pd.read_csv(url, compression='gzip', low_memory=False)
        if not df.empty:
            df_list.append(df)
            with open(log_file, 'a') as f:
                f.write(filename + '\n')
    except Exception as e:
        print(f"⚠️ Error reading {filename}: {e}")

# Combine and upload to Domo 
if df_list:
    new_data = pd.concat(df_list, ignore_index=True)
    print(f"⬆ Uploading {new_data.shape[0]} rows to Domo...")

    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmpfile:
        new_data.to_csv(tmpfile.name, index=False)
        domo.datasets.data_import_from_file(DATASET_ID, tmpfile.name)

    print("✅ Upload complete.")
