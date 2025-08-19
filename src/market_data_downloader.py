"""
market_data_downloader.py

Author: Srikar Kalle
Description: 
    Downloads historical daily closing prices for specified stock and commodity tickers
    from Yahoo Finance, based on parameters in a YAML config file. Saves the clean combined
    data as CSV in the data directory.

Usage:
    python market_data_downloader.py

"""

import os
import logging
import yaml
import yfinance as yf
import pandas as pd

def load_config(config_path=None):
    """Load YAML configuration file."""
    import os
    if config_path is None:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_dir, "..", "config.yaml")
        config_path = os.path.abspath(config_path)
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def download_market_data(tickers, start_date, end_date):
    """Download historical daily close prices for given tickers."""
    data = yf.download(tickers, start=start_date, end=end_date, group_by='ticker', progress=False)
    price_df = pd.DataFrame()

    for ticker in tickers:
        if ticker in data:
            df = data[ticker][["Close"]].copy()
            df.columns = [ticker]
            price_df = pd.concat([price_df, df], axis=1)
        else:
            logging.warning(f"Ticker {ticker} not found in downloaded data.")

    return price_df

def save_data(df, output_dir, filename):
    """Save DataFrame to CSV in specified output directory."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_path = os.path.join(output_dir, filename)
    df.index.name = "date"
    df.to_csv(output_path)
    logging.info(f"Saved data to {output_path}")

def main():
    """Main function to load config, download data, and save to CSV."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    config = load_config()

    tickers = config.get("tickers", [])
    start_date = config.get("startDate")
    end_date = config.get("endDate")
    output = config.get("output", {})
    output_dir = output.get("output_dir", "../data")
    filename = output.get("filename", "energy_market_prices.csv")

    logging.info("Starting download of market data...")
    price_df = download_market_data(tickers, start_date, end_date)

    if price_df.empty:
        logging.error("No data downloaded. Exiting.")
        return

    save_data(price_df, output_dir, filename)
    logging.info("Market data download and save completed.")

if __name__ == "__main__":
    main()