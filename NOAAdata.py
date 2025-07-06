import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from tqdm import tqdm

# URL of the NOAA storm events directory
base_url = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/'

# Get the page content
response = requests.get(base_url)
soup = BeautifulSoup(response.text, 'html.parser')

# Find all links ending in .csv.gz
file_links = [
    base_url + node.get('href') 
    for node in soup.find_all('a') 
    if node.get('href') and node.get('href').endswith('.csv.gz')
]

print(f"Found {len(file_links)} files to download.")

# Create local output folder
output_dir = "noaa_csvs"
os.makedirs(output_dir, exist_ok=True)

# Download files (with progress bar)
for link in tqdm(file_links[:5], desc="Downloading CSVs"):  # change [:5] to remove limit
    filename = link.split('/')[-1]
    file_path = os.path.join(output_dir, filename)
    
    # Skip already downloaded files
    if os.path.exists(file_path):
        continue
    
    with requests.get(link, stream=True) as r:
        r.raise_for_status()
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

print("✅ All files downloaded.")

