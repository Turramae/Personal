from pydomo import Domo
import pandas as pd

# Domo credentials
client_id = '152fe71a-bc33-42b3-a38b-e0e82615f71f'
client_secret = 'a5adb7f9e13561d3371971fa6883639c73c87189cdb626f363aad2449b487495'

# Authenticate
domo = Domo(client_id, client_secret, api_host='api.domo.com')

# Download NOAA CSV (gzip-compressed)
url = 'https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_details-ftp_v1.0_d2025_c20250520.csv.gz'
df = pd.read_csv(url, compression='gzip')

# Define schema based on DataFrame columns
schema = [{"name": col, "type": "STRING"} for col in df.columns]

# Create a new dataset in Domo
dataset = domo.datasets.create({
    "name": "NOAA Storm Events",
    "description": "Storm events from NCEI (2025 data)",
    "schema": {"columns": schema}
})

# Save the DataFrame to a temporary CSV file
with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmpfile:
    df.to_csv(tmpfile.name, index=False)
    domo.datasets.data_import_from_file(dataset['id'], tmpfile.name)
