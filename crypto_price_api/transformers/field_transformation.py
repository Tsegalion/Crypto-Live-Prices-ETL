import pandas as pd
import logging


if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    try:
        # Check if data loading was successful
        if data is not None:
            # Changing data types of selected columns to float
            columns_to_convert = ['supply', 'vwap24hr', 'volumeusd24hr', 'price', 'changepercent24hr']
            data[columns_to_convert] = data[columns_to_convert].astype(float)
            
            # Convert 'date_time' column to datetime format
            data['date_time'] = pd.to_datetime(data['date_time'])
            
        return data

    except Exception as e:
        logging.error(f"Error occurred during data transformation: {str(e)}")
        return None



@test
def test_output_float_conversion(output, *args):
    columns_to_convert = ['supply', 'vwap24hr', 'volumeusd24hr', 'price', 'changepercent24hr']
    
    # Check if the data types of specified columns have been changed to float
    converted_columns = output[columns_to_convert].dtypes
    assert all(converted_columns == 'float64'), 'The data types of columns_to_convert has not been changed to float'

    # If all assertions pass, the test is successful
    return True

@test
def test_output_datetime_conversion(output, *args):
    # Check if the 'date_time' column has been converted to datetime
    assert output['date_time'].dtype == 'datetime64[ns]', "The 'date_time' column has not been converted to datetime"

    # If all assertions pass, the test is successful
    return True