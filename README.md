## WORK IN PROGRESS ##

# NOAA Storm Events Scraper → Domo Pipeline

This project automates the ingestion of NOAA Storm Events data from the National Centers for Environmental Information (NCEI) public FTP site and uploads the cleaned dataset to a Domo DataSet using the Domo API.

## Overview

-  **Source**: [NOAA Storm Events FTP](https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/)
-  **Transform**: Clean and validate the storm event data
-  **Destination**: Pushes to a Domo dataset via the Domo Python SDK (`pydomo`)

---

## Features

- Automatically scrapes the most recent `.csv` file from the NOAA FTP directory
- Downloads and loads the file into a Pandas DataFrame
- Cleans and converts data types (e.g., float to integer for year fields)
- Uploads to Domo as a new or updated dataset
- Logs each run with timestamps and status messages

