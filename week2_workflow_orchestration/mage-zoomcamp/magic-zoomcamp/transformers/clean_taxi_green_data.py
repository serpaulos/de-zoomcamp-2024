import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def camel_to_snake(column_name):
    # Using a regular expression to insert an underscore before each uppercase letter
    import re
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', column_name).lower()


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    print("Preprocessing: rows with zero passengers: ", data['passenger_count'].isin([0]).sum())
    print("Preprocessing: rows with zero trip distance: ", data['trip_distance'].isin([0]).sum())
    data = (data[data['passenger_count'] != 0])
    data = (data[data['trip_distance'] != 0])
    print("Preprocessing: rows with zero passengers: ", data['passenger_count'].isin([0]).sum())
    print("Preprocessing: rows with zero trip distance: ", data['trip_distance'].isin([0]).sum())

    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # Replace columns with Snake Case names
    data.rename(columns={col: camel_to_snake(col) for col in data.columns}, inplace=True)

    data.columns = (data.columns.str.replace(' ', '_').str.lower())

    data['lpep_pickup_date'] = pd.to_datetime(data['lpep_pickup_date'])

    return data

@test
def test_output(data, *args) -> None:
    assert 'vendor_id' in data.columns, 'The output is undefined'

@test
def test_row_count(data, *args) -> None:
    assert (data['passenger_count'] > 0).any(), 'The output is undefined'

@test
def test_row_count(data, *args) -> None:
    assert (data['trip_distance'] > 0).any(), 'The output is undefined'