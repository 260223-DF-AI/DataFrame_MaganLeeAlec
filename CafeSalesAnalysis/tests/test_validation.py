import pytest
import pandas as pd
from src.sales_analysis import validation

@pytest.mark.parametrize(
        "data, column_str, data_type, expected_column, expected_assert", [({"Money": ["21.23", "100.57", "30.00"]}, "Money", float, {"Money": [21.23, 100.57, 30.00]}, True),
        ({"Money": ["21.23", "NaN", "30.00"]}, "Money", float, {"Money": [21.23, float("NaN"), 30.00]}, False)]
)
def test_change_col_dtype(data, column_str, data_type, expected_column, expected_assert):
    """Tests if a column in a dataframe will change to the declared data type"""
    #with pytest.raises(TypeError):
    actual = validation.change_col_dtype(data, column_str, data_type)

    # asserting with pandas will raise an assert error instead of checking if it matches the expected assert
    #pd.testing.assert_series_equal( actual[column_str], pd.DataFrame(expected_column)[column_str]) is expected_assert
    assert actual[column_str].equals(pd.DataFrame(expected_column)[column_str])