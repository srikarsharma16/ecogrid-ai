"""
weather_fetch.py

Author: Srikar Kalle

Description:
------------
This module fetches daily weather and solar data from NASA POWER API for a list of specified 
locations and a configurable date range. It extracts parameters such as surface solar irradiance,
2-meter temperature, 2-meter wind speed, and precipitation, and saves the data as CSV files
in a dedicated output directory.

The script reads configuration parameters such as locations, API settings, date range, and 
output directory from an external YAML configuration file (`config.yaml`).

Usage:
------
Run the module directly to start downloading weather data for all configured locations:

    python weather_fetch.py

Dependencies:
-------------
- requests
- pandas
- pyyaml

Ensure these are installed via pip before running the script.

Example:
--------
$ python weather_fetch.py

Output CSV files will be saved in the configured `data/` folder.
"""
import os
import time
import requests
import pandas as pd
import yaml


def load_config(config_path="../config.yaml"):  # relative path to your config.yaml
    """Load YAML configuration file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def fetch_weather_data(location, start_date, end_date, parameters, api_config):
    """Fetch weather data for a single location using NASA POWER API."""
    base_url = api_config["base_url"]
    community = api_config["community"]
    response_format = api_config["format"]

    url = (
        f"{base_url}"
        f"?start={start_date}&end={end_date}"
        f"&latitude={location['lat']}&longitude={location['lon']}"
        f"&community={community}"
        f"&parameters={','.join(parameters)}"
        f"&format={response_format}"
    )

    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def parse_weather_json(data, location_name):
    """Parse the JSON response from NASA POWER API into a pandas DataFrame."""
    recs = data["properties"]["parameter"]
    dates = list(recs[next(iter(recs))].keys())  # dates from any parameter key

    df = pd.DataFrame({
        "date": pd.to_datetime(dates),
        "county": location_name,
    })

    # Add each parameter as a column
    for param, values in recs.items():
        df[param] = [values[date] for date in dates]

    return df


def main():
    config = load_config()

    locations = config["locations"]
    start_date = config["start_date"]
    end_date = config["end_date"]
    parameters = config["parameters"]
    output_dir = config["output_dir"]
    api_config = config["api"]

    os.makedirs(output_dir, exist_ok=True)

    for loc in locations:
        try:
            print(f"Fetching data for {loc['name']}...")
            data = fetch_weather_data(loc, start_date, end_date, parameters, api_config)
            df = parse_weather_json(data, loc["name"])

            filename = os.path.join(output_dir, f"weather_{loc['name']}.csv")
            df.to_csv(filename, index=False)
            print(f"Saved: {filename}")

        except (requests.HTTPError, KeyError) as e:
            print(f"Failed for {loc['name']}: {e}")

        time.sleep(api_config.get("sleep_sec", 1))


if __name__ == "__main__":
    main()
