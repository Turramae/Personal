import requests
from bs4 import BeautifulSoup
from datetime import datetime
import os
import pandas as pd
from tqdm import tqdm

# Setup
base_url = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/'
output_dir = "noaa_csvs"
os.makedirs(output_dir, exist_ok=True)

# Get current date in format like: "2025-07-02"
today_str = datetime.utcnow().strftime('%Y-%b-%d') 

# Get the HTML index
response = requests.get(base_url)
soup = BeautifulSoup(response.text, 'html.parser')

# Find all links with their parent row's text (includes date)
rows = soup.find_all('tr')

# Track downloaded files
file_links = []

for row in rows:
    cells = row.find_all('td')
    if len(cells) >= 2:
        link_tag = cells[0].find('a')
        date_text = cells[1].text.strip()
        
        if link_tag and link_tag.get('href', '').endswith('.csv.gz'):
            file_name = link_tag.get('href')
            file_url = base_url + file_name
            
            if today_str in date_text:
                file_links.append(file_url)

print(f"🆕 Found {len(file_links)} new file(s) modified today ({today_str}).")

# Download each file
for url in tqdm(file_links, desc="Downloading today's CSVs"):
    filename = url.split('/')[-1]
    file_path = os.path.join(output_dir, filename)

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

print("✅ Done downloading today's NOAA files.")
