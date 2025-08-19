"""
data_preprocessing.py

Author: Srikar Kalle
Description:
    Reads all weather CSV files with prefix 'weather_' from the data directory,
    concatenates them into a single master DataFrame, and saves as a master CSV.
"""

import os
import logging
import pandas as pd

def combine_weather_files(data_dir):
    """Read all weather_*.csv files from data_dir, combine, and return DataFrame."""
    csv_files = [f for f in os.listdir(data_dir) if f.startswith("weather_") and f.endswith(".csv")]

    if not csv_files:
        logging.warning(f"No files found in {data_dir} matching 'weather_*.csv'")
        return pd.DataFrame()

    df_list = []
    for file in csv_files:
        file_path = os.path.join(data_dir, file)
        logging.info(f"Reading {file_path}")
        df = pd.read_csv(file_path)
        df_list.append(df)

    combined_df = pd.concat(df_list, ignore_index=True)
    return combined_df

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    # You can modify this path or load from config.yaml
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))

    logging.info(f"Combining weather CSV files from: {data_dir}")
    combined_df = combine_weather_files(data_dir)

    if combined_df.empty:
        logging.error("No data combined. Exiting.")
        return

    logging.info(f"Combined shape: {combined_df.shape}")
    if "county" in combined_df.columns:
        logging.info(f"Unique counties: {combined_df['county'].nunique()}")
    if "date" in combined_df.columns:
        logging.info(f"Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")

    output_path = os.path.join(data_dir, "ireland_weather_master.csv")
    combined_df.to_csv(output_path, index=False)
    logging.info(f"Saved master file as {output_path}")

if __name__ == "__main__":
    main()
