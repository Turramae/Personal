import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from tqdm import tqdm
from pydomo import Domo
import tempfile

# Set up Domo credentials 
client_id = '152fe71a-bc33-42b3-a38b-e0e82615f71f'
client_secret = 'a5adb7f9e13561d3371971fa6883639c73c87189cdb626f363aad2449b487495'
domo = Domo(client_id, client_secret, api_host='api.domo.com')

# Scrape NOAA file URLs 
base_url = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/'
response = requests.get(base_url)
soup = BeautifulSoup(response.text, 'html.parser')

file_links = [
    base_url + node.get('href') 
    for node in soup.find_all('a') 
    if node.get('href') and node.get('href').endswith('.csv.gz')
]

print(f"Found {len(file_links)} files.")

# Download and combine into one DataFrame 
output_dir = "noaa_csvs"
os.makedirs(output_dir, exist_ok=True)
df_list = []

for link in tqdm(file_links, desc="Downloading & Reading CSVs"):
    filename = link.split('/')[-1]
    file_path = os.path.join(output_dir, filename)

    # Download if not already saved
    if not os.path.exists(file_path):
        with requests.get(link, stream=True) as r:
            r.raise_for_status()
            with open(file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

    # Read CSV into pandas
    try:
        df = pd.read_csv(file_path, compression='gzip', low_memory=False)
        df_list.append(df)
    except Exception as e:
        print(f"⚠️ Could not read {filename}: {e}")

# Combine all into a single DataFrame
combined_df = pd.concat(df_list, ignore_index=True)
print(f"✅ Combined DataFrame shape: {combined_df.shape}")

# Create schema for Domo upload 
schema = [{"name": col, "type": "STRING"} for col in combined_df.columns]

# Create new dataset in Domo 
dataset = domo.datasets.create({
    "name": "NOAA Storm Events",
    "description": "All storm event data from NOAA (multi-year)",
    "schema": {"columns": schema}
})

# Upload data using a temporary CSV file 
with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmpfile:
    combined_df.to_csv(tmpfile.name, index=False)
    domo.datasets.data_import_from_file(dataset['id'], tmpfile.name)

print("✅ Upload complete. Check your Domo Data Center.")
