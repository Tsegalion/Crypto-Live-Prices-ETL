import pandas as pd
import requests
from datetime import datetime
import logging
from mage_ai.io.config import ConfigFileLoader

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data_from_coincap_api(*args, **kwargs):
    try:
        # Initialize the ConfigFileLoader object with the path to the configuration file and profile to retrieve the API key
        config_loader = ConfigFileLoader(filepath='crypto_price_api\io_config.yaml', profile='default')
        config = config_loader.config
        api_key = config.get('api_key')

        baseurl = 'https://api.coincap.io/v2/'
        endpoint = 'assets'

        headers = {
            'Authorization': f'Bearer {api_key}'
        }
        
        # Initialize variables for pagination
        offset = 0
        page_size = 100
        
        # Initialize an empty list to store DataFrames from each chunk
        dfs = []
        
        while True:
            # Make an HTTP GET request to the API for retrieving data
            params = {'offset': offset, 'limit': page_size}
            price_data = requests.get(baseurl + endpoint, headers=headers, params=params)
            
            # Parse the JSON data
            data = price_data.json()['data']
            
            # If no data is returned or the data is empty, break the loop
            if not data:
                break
            
            # Process each chunk of data
            extracted_data = []
            for asset in data:
                extracted_data.append({
                    'id': asset['id'],
                    'date_time': datetime.now(),
                    'supply': asset['supply'],
                    'volumeusd24hr': asset['volumeUsd24Hr'],
                    'vwap24hr': asset["vwap24Hr"],
                    'price': asset['priceUsd'],
                    'changepercent24hr': asset['changePercent24Hr']
                })
            
            # Creating a DataFrame from the extracted data
            df_chunk = pd.DataFrame(extracted_data)
            
            # Append the DataFrame from this chunk to the list
            dfs.append(df_chunk)
            
            # Increment the offset for the next page
            offset += page_size
        
        # Concatenate all DataFrames in the list to create a single DataFrame
        asset_update = pd.concat(dfs, ignore_index=True)
        
        return asset_update

    except Exception as e:
        logging.error(f"Error occurred during data loading: {str(e)}")
        return None


@test
def test_output(output, *args):
    assert output is not None, 'The output is undefined'

    # Ensure the output DataFrame has the expected columns
    expected_columns = ['id', 'date_time', 'supply', 'volumeusd24hr', 'vwap24hr', 'price', 'changepercent24hr']
    assert all(col in output.columns for col in expected_columns), "Output DataFrame doesn't have the expected columns"
