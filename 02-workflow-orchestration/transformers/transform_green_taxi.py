import re
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    # Remove rows where the passenger count is equal to 0 and the trip distance is equal to zero.
    data = data[(data["passenger_count"] > 0) & (data["trip_distance"] > 0)]

    print("Question 2. Data Transformation - nb rows after filter", len(data))

    # New column lpep_pickup_date by converting lpep_pickup_datetime to a date
    print("Question 3. Data Transformation - lpep_pickup_datetime to date")
    data["lpep_pickup_date"] = data["lpep_pickup_datetime"].dt.date


    print("Question 4. Data Transformation - values of VendorID in the dataset", data["VendorID"].unique())

    print(
        "Question 5. Data Transformation - how many columns need to be renamed to snake case?",
        len(list(filter(lambda col: re.search('(?<=[a-z])(?=[A-Z])', col), data.columns)))
    )
    # Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
    data.columns = (
        data.columns
        .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
        .str.lower()
    )

    return data

@test
def test_output(output, *args):

    assert "vendor_id" in output.columns, "Column vendor_id is not present"

    assert output['passenger_count'].isin([0]).sum() == 0, "There are rides with zero passengers"

    assert output['trip_distance'].isin([0]).sum() == 0, "There are rides with zero trip distance"

