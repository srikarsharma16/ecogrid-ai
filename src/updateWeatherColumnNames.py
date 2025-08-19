"""
updateWeatherColumnNames.py

Author: Srikar Kalle
Description:
    Reads all weather_*.csv files in the ../data/ directory,
    renames columns to the desired names, reorders them,
    and overwrites the original CSV files.
    
Usage:
    python updateWeatherColumnNames.py
"""

import os
import pandas as pd
import glob

# Directory where your weather CSV files are stored
DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))

# Column mapping: old column names -> new column names
COLUMN_MAPPING = {
    "T2M": "temperature",
    "ALLSKY_SFC_SW_DWN": "solar_irradiance",
    "WS2M": "wind_speed",
    "PRECTOTCORR": "precipitation",
    "date": "date",
    "county": "county"
}

# Desired column order
DESIRED_ORDER = ["date", "county", "temperature", "solar_irradiance", "wind_speed", "precipitation"]

def update_csv(file_path):
    """Read, rename, reorder, and overwrite CSV."""
    df = pd.read_csv(file_path)
    
    # Rename columns
    df = df.rename(columns=COLUMN_MAPPING)
    
    # Reorder columns
    df = df[DESIRED_ORDER]
    
    # Overwrite CSV
    df.to_csv(file_path, index=False)
    print(f"✅ Updated: {file_path}")

def main():
    csv_files = glob.glob(os.path.join(DATA_DIR, "weather_*.csv"))
    if not csv_files:
        print(f"No weather CSV files found in {DATA_DIR}.")
        return
    
    for file_path in csv_files:
        update_csv(file_path)

if __name__ == "__main__":
    main()