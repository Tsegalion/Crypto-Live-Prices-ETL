import requests
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
from pandas_gbq import to_gbq

load_dotenv()
# Function to fetch cryptocurrency data from CoinCap
def fetch_data(api_key):
    # API endpoint URL
    baseurl = 'https://api.coincap.io/v2/'
    endpoint = 'assets'

    # Inputing the API key in the request headers
    headers = {
        'Authorization': f'Bearer {api_key}'
    }
    
    # Making an HTTP GET request to the API for retrieving data
    price_data = requests.get(baseurl + endpoint, headers=headers)
    
    # Parsing the JSON data
    return price_data.json()['data']

# Function to extract relevant data from the API response
def extract_data(asset):
    # Get the current date and time
    return {
        'id' : asset['id'],
        'date_time' : datetime.now(),
        'supply' : asset['supply'],
        'volumeusd24hr' : asset['volumeUsd24Hr'],
        'vwap24hr': asset["vwap24Hr"],
        'price' : asset['priceUsd'],
        'changepercent24hr' : asset['changePercent24Hr']
    }

# Main function to orchestrate the ETL process
def main():
    # Access environment variables
    api_key = os.getenv("api_key")
    project_id = os.getenv("GCP_PROJECT_ID")

    # Fetching cryptocurrency data from the CoinCap API
    data = fetch_data(api_key)
    
    # Extracting data for each asset in the API response
    coin = [extract_data(asset) for asset in data]
    
    # Creating a DataFrame from the extracted data
    asset_update = pd.DataFrame(coin)

    # TRANFORMATION 
    # Changing data types of selected columns to float
    columns_to_convert = ['supply', 'vwap24hr', 'volumeusd24hr', 'price', 'changepercent24hr']
    asset_update[columns_to_convert] = asset_update[columns_to_convert].astype(float)

    # Convert 'date_time' column to datetime format
    asset_update['date_time'] = pd.to_datetime(asset_update['date_time'])

    # Loading the DataFrame to Google BigQuery
    asset_update.to_gbq('crypto-pipeline-398422.asset_updates.assets', project_id, if_exists='append')


if __name__ == "__main__":
    main()