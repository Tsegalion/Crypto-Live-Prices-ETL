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
        
        # API endpoint URL
        baseurl = 'https://api.coincap.io/v2/'
        endpoint = 'assets'

        # Inputting the API key in the request headers
        headers = {
            'Authorization': f'Bearer {api_key}'
        }
        
        # Making an HTTP GET request to the API for retrieving data
        price_data = requests.get(baseurl + endpoint, headers=headers)
        
        # Parsing the JSON data
        data = price_data.json()['data']
        
        # Get the current date and time
        date_time = datetime.now()
        
        # Extracting relevant data from the API response
        coin = []
        for asset in data:
            currency = asset['name']
            total_supply = asset['supply']
            volumeUsd24Hr = asset['volumeUsd24Hr']
            price = asset['priceUsd']
            vwap24Hr = asset["vwap24Hr"]
            changePercent24Hr = asset['changePercent24Hr']

            coin.append({
                'id': currency,
                'date_time': date_time,
                'supply': total_supply,
                'volumeusd24hr': volumeUsd24Hr,
                'vwap24hr': vwap24Hr,
                'price': price,
                'changepercent24hr': changePercent24Hr
            })
        
        # Convert the extracted data to a DataFrame
        df = pd.DataFrame(coin)
        
        return df

    except Exception as e:
        logging.error(f"Error occurred during data loading: {str(e)}")
        return None


@test
def test_output(output, *args):
    assert output is not None, 'The output is undefined'

    # Ensure the output DataFrame has the expected columns
    expected_columns = ['id', 'date_time', 'supply', 'volumeusd24hr', 'vwap24hr', 'price', 'changepercent24hr']
    assert all(col in output.columns for col in expected_columns), "Output DataFrame doesn't have the expected columns"

    # Ensure all columns are populated with data (no missing values)
    assert not output['price'].isnull().values.any(), "Output price contains missing values"