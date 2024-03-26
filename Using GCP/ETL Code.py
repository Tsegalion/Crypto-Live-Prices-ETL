import requests
import pandas as pd
from datetime import datetime
from pandas_gbq import to_gbq

# Project ID
project_id = 'crypto-pipeline-398422'

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
    currency = asset['name']
    
    # Getting the current date and time
    date_time = datetime.now()
   
    total_supply = asset['supply']
    volumeUsd24Hr = asset['volumeUsd24Hr']
    price = asset['priceUsd']
    changePercent24Hr = asset['changePercent24Hr']

    # Returning a dictionary that contains the extracted data
    return {
        'currency' : currency,
        'date_time' : date_time,
        'total_supply' : total_supply,
        'volumeUsd24Hr' : volumeUsd24Hr,
        'price' : price,
        'changePercent24Hr' : changePercent24Hr
    }

# Function to call the CoinCap API and process the data
def call():
    # Fetching cryptocurrency data from the CoinCap API
    data = fetch_data('COIN_CAP_API')
    
    # Extracting data for each asset in the API response
    coin = [extract_data(asset) for asset in data]
    
    # Creating a DataFrame from the extracted data
    df = pd.DataFrame(coin)
    
    return df

# Main function that orchestrates the data processing and loading to BigQuery
def main():
    # Assigned a variable name to the call function
    asset_update = call()

    # TRANFORMATION 
    # Changing data types of selected columns to float
    columns_to_convert = ['total_supply', 'volumeUsd24Hr', 'price', 'changePercent24Hr']
    asset_update[columns_to_convert] = asset_update[columns_to_convert].astype(float)

    # Loading the DataFrame to Google BigQuery
    asset_update.to_gbq('crypto-pipeline-398422.asset_updates.assets', project_id, if_exists='append')

# This block ensures that the 'main' function is executed when the script is run
if __name__ == "__main__":
    main()
